package placement

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

func TestSingleRestorationRequiresPhysicalAbsence(t *testing.T) {
	for _, tt := range []struct {
		name      string
		edit      func(*plannedObservationFixture, *placementObservations, *[]string)
		resources bool
		restore   bool
		wantErr   bool
	}{
		{name: "absent original resolves fresh policy", restore: true},
		{name: "restart retains original budget after release", restore: true, edit: func(f *plannedObservationFixture, _ *placementObservations, _ *[]string) {
			a := f.source.Status.Placement.Candidates[0].Allocation
			a.CurrentReplicas, a.CurrentHome = 0, nil
		}},
		{name: "unknown original retains current policy", edit: func(_ *plannedObservationFixture, o *placementObservations, _ *[]string) {
			o.homes["member-a"] = homeObservation{state: homeUnknown}
		}},
		{name: "member reappeared after initial observation", resources: true},
		{name: "terminating member retains authority", resources: true, edit: func(f *plannedObservationFixture, _ *placementObservations, _ *[]string) {
			f.member.Finalizers = []string{"example.com/cleanup"}
			f.member.DeletionTimestamp = ptr.To(metav1.Now())
		}},
		{name: "remaining components retain authority", resources: true, edit: func(f *plannedObservationFixture, _ *placementObservations, _ *[]string) {
			f.member = nil
		}},
		{name: "orphan pod cannot prove absence", resources: true, wantErr: true, edit: func(f *plannedObservationFixture, _ *placementObservations, _ *[]string) {
			f.member, f.resources.ir = nil, nil
		}},
		{name: "failed pod inventory holds restoration", wantErr: true, edit: func(f *plannedObservationFixture, _ *placementObservations, _ *[]string) {
			f.intercept.List = func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*corev1.PodList); ok {
					return fmt.Errorf("pod inventory unavailable")
				}
				return cl.List(ctx, list, opts...)
			}
		}},
		{name: "ineligible original cannot be recreated", wantErr: true, edit: func(_ *plannedObservationFixture, _ *placementObservations, eligible *[]string) {
			*eligible = nil
		}},
		{name: "unresolved new floor holds restoration", wantErr: true, edit: func(f *plannedObservationFixture, _ *placementObservations, _ *[]string) {
			f.source.Spec.Engine.MinReplicas = ptr.To(0)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := observationFixture(t)
			source := fixture.source
			source.Spec.Placement.Mode, source.Spec.Placement.Split = v1beta1.PlacementModeSingle, nil
			source.Spec.Engine.MinReplicas = ptr.To(2)
			source.Status.Placement.Plan.Mode, source.Status.Placement.Plan.Winner = v1beta1.PlacementModeSingle, "member-a"
			source.Status.Placement.Plan.SingleMove = &v1beta1.PlacementSingleMoveStatus{Selected: "member-b"}
			a := source.Status.Placement.Candidates[0].Allocation
			a.OriginalReplicas = 1
			a.CurrentHome = &v1beta1.PlacementHomePolicy{InputDigest: "accepted-intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 1}}}
			a.DesiredReplicas, a.DesiredHome = 0, nil
			b := a.DeepCopy()
			b.ClusterUID, b.OriginalReplicas = "member-b-uid", 0
			b.DesiredReplicas, b.DesiredHome = 1, b.CurrentHome.DeepCopy()
			source.Status.Placement.Candidates = append(source.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: "member-b", Allocation: b})
			standing := &placementObservations{matches: map[string]bool{"member-a": true}, homes: map[string]homeObservation{"member-a": {state: homeAbsent}}}
			eligible := []string{"member-a"}
			if tt.edit != nil {
				tt.edit(&fixture, standing, &eligible)
			}
			scheme := testScheme(t)
			worker := emptyWorker(scheme)
			if tt.resources {
				var objects []client.Object
				if fixture.member != nil {
					objects = append(objects, fixture.member)
				}
				if fixture.resources.ir != nil {
					objects = append(objects, fixture.resources.ir)
				}
				for i := range fixture.resources.pods {
					objects = append(objects, &fixture.resources.pods[i])
				}
				for _, object := range objects {
					if err := worker.Create(t.Context(), object); err != nil {
						t.Fatal(err)
					}
				}
			}
			connections := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
				"member-a": workloadcluster.NewNeverCachingClient(interceptor.NewClient(worker, fixture.intercept)),
				"member-b": workloadcluster.NewNeverCachingClient(emptyWorker(scheme)),
			}}
			clusters := []v1beta1.WorkloadCluster{*readyWC("member-a", nil), *readyWC("member-b", nil)}
			r, _ := newPlacer(scheme, connections, &clusters[0], &clusters[1])
			before := source.DeepCopy()
			got, err := r.singleMoveProposal(t.Context(), source, clusters, eligible, standing)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("error %v: %s", err, diff)
			}
			if diff := cmp.Diff(before, source); diff != "" {
				t.Fatalf("source mutated: %s", diff)
			}
			if tt.wantErr {
				return
			}
			want := *before.Status.Placement.Candidates[0].Allocation.DeepCopy()
			want.Matched = true
			want.DesiredReplicas, want.DesiredHome = want.CurrentReplicas, want.CurrentHome.DeepCopy()
			if tt.restore {
				want.CurrentReplicas, want.CurrentHome = 0, nil
				want.DesiredReplicas = 2
				want.DesiredHome = &v1beta1.PlacementHomePolicy{InputDigest: got.InputDigest, ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 2}}}
			}
			if diff := cmp.Diff(want, got.Assignments["member-a"]); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff("member-a", got.SingleMove.Selected); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(int32(0), got.Assignments["member-b"].DesiredReplicas); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
