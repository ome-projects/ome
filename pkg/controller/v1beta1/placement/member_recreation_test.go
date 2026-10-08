package placement

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/plan"
)

func TestPlannedMemberRecreationWaitsForInventory(t *testing.T) {
	for _, mode := range []v1beta1.PlacementMode{v1beta1.PlacementModeSingle, v1beta1.PlacementModeAll, v1beta1.PlacementModeSplit} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tt := range []struct {
				name           string
				components     bool
				pods           bool
				failList       bool
				lateComponent  bool
				latePod        bool
				failRefresh    bool
				partialRefresh bool
				wantCreate     bool
			}{
				{name: "empty inventory permits recreation", wantCreate: true},
				{name: "orphan component and pod block recreation", components: true, pods: true},
				{name: "orphan component reserves uncreated replicas", components: true},
				{name: "orphan pod blocks recreation", pods: true},
				{name: "failed inventory blocks recreation", failList: true},
				{name: "component appears after planning inventory", lateComponent: true},
				{name: "pod appears after planning inventory", latePod: true},
				{name: "creation inventory fails after planning", failRefresh: true},
				{name: "creation inventory has an unread page", partialRefresh: true},
			} {
				t.Run(tt.name, func(t *testing.T) {
					f := observationFixture(t)
					f.source.Spec.Placement.Mode = mode
					if mode != v1beta1.PlacementModeSplit {
						f.source.Spec.Placement.Split = nil
					} else {
						f.source.Spec.Placement.Split.Replicas = ptr.To[int32](1)
					}
					f.source.Spec.Engine.MinReplicas = ptr.To(1)
					a := f.source.Status.Placement.Candidates[0].Allocation
					a.Matched, a.OriginalReplicas = true, 1
					a.CurrentHome = &v1beta1.PlacementHomePolicy{InputDigest: "accepted-input", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 1}}}
					a.DesiredHome = a.CurrentHome.DeepCopy()
					proposal := plan.Proposal{Mode: mode, InputDigest: "accepted-input", PauseSurge: true, Assignments: map[string]v1beta1.CandidateAllocationStatus{"member-a": *a.DeepCopy()}}
					if mode == v1beta1.PlacementModeSingle {
						proposal.Winner = "member-a"
						proposal.SingleMove = &v1beta1.PlacementSingleMoveStatus{Selected: "member-a"}
					}
					scheme := testScheme(t)
					worker := emptyWorker(scheme)
					var objects []client.Object
					if tt.components {
						objects = append(objects, f.resources.ir)
					}
					if tt.pods {
						objects = append(objects, &f.resources.pods[0])
					}
					for _, object := range objects {
						if err := worker.Create(t.Context(), object); err != nil {
							t.Fatal(err)
						}
					}
					creates := 0
					inventoryRead, injected := false, false
					failList := tt.failList
					failRefresh, partialRefresh := tt.failRefresh, tt.partialRefresh
					wrapped := interceptor.NewClient(worker, interceptor.Funcs{
						Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, object client.Object, opts ...client.GetOption) error {
							if _, service := object.(*v1beta1.InferenceService); service && (tt.lateComponent || tt.latePod) && inventoryRead && !injected {
								var remaining client.Object = f.resources.ir
								if tt.latePod {
									remaining = &f.resources.pods[0]
								}
								if err := c.Create(ctx, remaining); err != nil {
									return err
								}
								injected = true
							}
							return c.Get(ctx, key, object, opts...)
						},
						Create: func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.CreateOption) error {
							if _, service := object.(*v1beta1.InferenceService); service {
								creates++
							}
							return c.Create(ctx, object, opts...)
						},
						List: func(ctx context.Context, c client.WithWatch, objects client.ObjectList, opts ...client.ListOption) error {
							if components, ok := objects.(*metav1.PartialObjectMetadataList); ok && components.Kind == "InferenceReplicaList" && inventoryRead {
								if failRefresh {
									return fmt.Errorf("creation inventory unavailable")
								}
								if partialRefresh {
									components.Continue = "next-page"
									return nil
								}
							}
							if _, pods := objects.(*corev1.PodList); pods {
								if failList {
									return fmt.Errorf("member inventory unavailable")
								}
								inventoryRead = true
							}
							return c.List(ctx, objects, opts...)
						},
					})
					connections := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(wrapped)}}, uid: "member-a-uid"}
					r, root := newPlacer(scheme, connections, f.source, plannedTestRegistration())
					if err := root.Get(t.Context(), client.ObjectKeyFromObject(f.source), f.source); err != nil {
						t.Fatal(err)
					}
					if _, err := r.executePlannedAllocation(t.Context(), f.source, []string{"member-a"}, &placementObservations{}, proposal, nil); err != nil {
						t.Fatal(err)
					}
					wantCreates := 0
					if tt.wantCreate {
						wantCreates = 1
					}
					if diff := cmp.Diff(wantCreates, creates); diff != "" {
						t.Errorf("member creation requests (-want +got):\n%s", diff)
					}
					member := &v1beta1.InferenceService{}
					err := worker.Get(t.Context(), client.ObjectKeyFromObject(f.source), member)
					if err != nil && !apierrors.IsNotFound(err) {
						t.Fatal(err)
					}
					if diff := cmp.Diff(tt.wantCreate, err == nil); diff != "" {
						t.Errorf("member exists (-want +got):\n%s", diff)
					}
					if tt.wantCreate {
						return
					}
					if tt.components || tt.lateComponent {
						live := &v1beta1.InferenceReplica{}
						if err := worker.Get(t.Context(), client.ObjectKeyFromObject(f.resources.ir), live); err != nil {
							t.Fatal(err)
						}
						if diff := cmp.Diff(f.resources.ir.Spec, live.Spec); diff != "" {
							t.Errorf("retained component authority changed: %s", diff)
						}
						if err := worker.Delete(t.Context(), live); err != nil {
							t.Fatal(err)
						}
					}
					if tt.pods || tt.latePod {
						if err := worker.Delete(t.Context(), &f.resources.pods[0]); err != nil {
							t.Fatal(err)
						}
					}
					failList, failRefresh, partialRefresh = false, false, false
					if err := root.Get(t.Context(), client.ObjectKeyFromObject(f.source), f.source); err != nil {
						t.Fatal(err)
					}
					if _, err := r.executePlannedAllocation(t.Context(), f.source, []string{"member-a"}, &placementObservations{}, proposal, nil); err != nil {
						t.Fatal(err)
					}
					if diff := cmp.Diff(1, creates); diff != "" {
						t.Errorf("recreation after cleanup (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}
