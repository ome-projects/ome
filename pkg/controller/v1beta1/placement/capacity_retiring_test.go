package placement

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

func TestRetiringCapacityMemberPreservesRendering(t *testing.T) {
	for _, tt := range []struct {
		name                                        string
		ceiling                                     int32
		template, hardware                          bool
		growth, collision, newerPolicy, staleSource bool
		wantErr                                     bool
	}{
		{name: "unchanged inputs"},
		{name: "explicit local ceiling", ceiling: 4},
		{name: "new source template", template: true},
		{name: "outgoing hardware changed", hardware: true},
		{name: "cannot grow retiring floor", growth: true, wantErr: true},
		{name: "cannot change local service", collision: true, wantErr: true},
		{name: "newer member authority", newerPolicy: true, wantErr: true},
		{name: "source changed before patch", staleSource: true, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newCapacityProviderFixture(t)
			samples, err := f.r.readSplitCapacity(t.Context(), f.source, f.clusters, f.clock.Now())
			if err != nil {
				t.Fatal(err)
			}
			evidence, err := capacityEvidence(samples["member-a"])
			if err != nil {
				t.Fatal(err)
			}
			member, err := f.r.derivedFor(f.source)
			if err != nil {
				t.Fatal(err)
			}
			setPlannedReplicas(member, 2, tt.ceiling)
			worker := f.workers["member-a"]
			if err := worker.Create(t.Context(), member); err != nil {
				t.Fatal(err)
			}
			want := member.DeepCopy()
			if !tt.wantErr {
				want.Spec.Engine.MinReplicas = ptr.To(1)
				want.Spec.Engine.MaxReplicas = 1
				if tt.ceiling > 0 {
					want.Spec.Engine.MaxReplicas = int(tt.ceiling)
				}
			}
			source := f.source.DeepCopy()
			source.Spec.Placement.Split.MaxReplicasPerCluster = tt.ceiling
			if tt.template {
				source.Spec.Engine.Runner = &v1beta1.RunnerSpec{Container: corev1.Container{Image: "example.com/serving:v2"}}
			}
			if tt.hardware {
				root := &v1beta1.AcceleratorQuota{}
				if err := worker.Get(t.Context(), client.ObjectKey{Name: providerConfig().RootName}, root); err != nil {
					t.Fatal(err)
				}
				root.Status.Capacity[0].Allocatable = resource.MustParse("0")
				if err := worker.Update(t.Context(), root); err != nil {
					t.Fatal(err)
				}
			}
			source.Status.Placement = &v1beta1.PlacementStatus{
				Plan:       &v1beta1.PlacementPlanStatus{ID: "plan-a", Revision: 1, SourceUID: source.UID, ObservedGeneration: source.Generation},
				Candidates: []v1beta1.CandidatePlacement{{Cluster: "member-a", Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: f.clusters[0].UID, OriginalReplicas: 2, CurrentReplicas: 1, DesiredReplicas: 0, Capacity: evidence}}},
			}
			if tt.growth {
				source.Status.Placement.Candidates[0].Allocation.CurrentReplicas = 3
			}
			if tt.collision {
				delete(member.Labels, constants.PlacementOrigin)
				delete(member.Annotations, constants.PlacementOriginUID)
			}
			if tt.newerPolicy {
				policy := executionPolicy(source, source.Status.Placement.Candidates[0].Allocation)
				policy.Revision++
				raw, err := protocol.Encode(policy)
				if err != nil {
					t.Fatal(err)
				}
				if member.Annotations == nil {
					member.Annotations = map[string]string{}
				}
				member.Annotations[constants.PlacementExecution] = raw
			}
			if tt.collision || tt.newerPolicy {
				if err := worker.Update(t.Context(), member); err != nil {
					t.Fatal(err)
				}
			}
			r, _ := newPlacer(f.r.Scheme, f.r.Clusters, source, f.clusters[0].DeepCopy())
			r.Capacity, r.CapacityClock = f.r.Capacity, f.clock
			if tt.staleSource {
				reads := 0
				r.APIReader = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if err := cl.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					if live, ok := obj.(*v1beta1.InferenceService); ok {
						reads++
						if reads == 2 {
							live.Generation++
						}
					}
					return nil
				}})
			}
			err = r.placePlannedOn(t.Context(), source, source.Status.Placement.Candidates[0])
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("retirement: %s: %v", diff, err)
			}
			got := &v1beta1.InferenceService{}
			if err := worker.Get(t.Context(), client.ObjectKeyFromObject(member), got); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(want.Spec, got.Spec); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
