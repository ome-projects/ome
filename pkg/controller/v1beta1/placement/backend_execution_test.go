package placement

import (
	"context"
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
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

func (f *backendFixture) runtimeMode(t *testing.T, name string, mode constants.DeploymentModeType) {
	t.Helper()
	runtime := backendTestRuntime()
	if err := f.workers[name].Get(t.Context(), client.ObjectKeyFromObject(runtime), runtime); err != nil {
		t.Fatal(err)
	}
	runtime.Spec.EngineConfig = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: string(mode)}}}
	if err := f.workers[name].Update(t.Context(), runtime); err != nil {
		t.Fatal(err)
	}
}

func (f *backendFixture) reconcile(t *testing.T) *v1beta1.InferenceService {
	t.Helper()
	key := client.ObjectKeyFromObject(f.source)
	if _, err := f.reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	got := &v1beta1.InferenceService{}
	if err := f.reconciler.Get(t.Context(), key, got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestBackendEligibilityControlsMemberWrites(t *testing.T) {
	for _, mode := range []v1beta1.PlacementMode{v1beta1.PlacementModeSingle, v1beta1.PlacementModeAll, v1beta1.PlacementModeSplit} {
		t.Run(string(mode), func(t *testing.T) {
			f := newBackendFixture(t, mode)
			f.runtimeMode(t, "member-b", constants.RawDeployment)
			got := f.reconcile(t)
			condition := got.Status.GetCondition(v1beta1.PlacementBackendReady)
			if condition == nil || condition.Status != corev1.ConditionFalse {
				t.Fatalf("unsupported backend condition = %+v", condition)
			}
			for _, name := range []string{"member-a", "member-b"} {
				member := &v1beta1.InferenceService{}
				err := f.workers[name].Get(t.Context(), client.ObjectKeyFromObject(f.source), member)
				if name == "member-b" {
					if !apierrors.IsNotFound(err) {
						t.Fatalf("unsupported member created: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				} else if mode == v1beta1.PlacementModeSplit {
					if diff := cmp.Diff(ptr.To(1), member.Spec.Engine.MinReplicas); diff != "" {
						t.Fatalf("eligible floor (-want +got):\n%s", diff)
					}
				}
			}
			if mode == v1beta1.PlacementModeSplit {
				desired, current := map[string]int32{}, map[string]int32{}
				for _, candidate := range got.Status.Placement.Candidates {
					desired[candidate.Cluster], current[candidate.Cluster] = candidate.Allocation.DesiredReplicas, candidate.Allocation.CurrentReplicas
				}
				if diff := cmp.Diff(map[string]int32{"member-a": 1, "member-b": 1}, desired); diff != "" {
					t.Fatalf("assigned shares (-want +got):\n%s", diff)
				}
				if diff := cmp.Diff(map[string]int32{"member-a": 1, "member-b": 0}, current); diff != "" {
					t.Fatalf("applied shares (-want +got):\n%s", diff)
				}
			}
			f.runtimeMode(t, "member-b", constants.OMENative)
			got = f.reconcile(t)
			condition = got.Status.GetCondition(v1beta1.PlacementBackendReady)
			if condition == nil || condition.Status != corev1.ConditionTrue {
				t.Fatalf("recovered backend condition = %+v", condition)
			}
		})
	}
}

func TestBackendWriteRevalidatesInputs(t *testing.T) {
	for _, planned := range []bool{false, true} {
		name := "unplanned"
		if planned {
			name = "planned"
		}
		t.Run(name, func(t *testing.T) {
			for _, tt := range []struct {
				name string
				edit func(*testing.T, *backendFixture)
				race bool
			}{
				{name: "runtime becomes unsupported", edit: func(t *testing.T, f *backendFixture) { f.runtimeMode(t, "member-a", constants.RawDeployment) }},
				{name: "runtime changes during resolution", race: true, edit: func(t *testing.T, f *backendFixture) { f.runtimeMode(t, "member-a", constants.RawDeployment) }},
				{name: "connection changes during resolution", race: true, edit: func(t *testing.T, f *backendFixture) {
					f.connections.m["member-a"] = workloadcluster.NewNeverCachingClient(emptyWorker(testScheme(t)))
				}},
				{name: "source changes during resolution", race: true, edit: func(t *testing.T, f *backendFixture) {
					t.Helper()
					current := &v1beta1.InferenceService{}
					if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), current); err != nil {
						t.Fatal(err)
					}
					current.Spec.Engine.Runner.Image = "updated-image"
					current.Generation++
					if err := f.reconciler.Update(t.Context(), current); err != nil {
						t.Fatal(err)
					}
				}},
			} {
				t.Run(tt.name, func(t *testing.T) {
					f := newBackendFixture(t, v1beta1.PlacementModeSingle)
					if planned {
						f.source.Spec.Placement.Mode = v1beta1.PlacementModeSplit
						f.source.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](2)}
						if err := f.reconciler.Update(t.Context(), f.source); err != nil {
							t.Fatal(err)
						}
						f.source.Status.Placement = plannedTestSource().Status.Placement
						if err := f.reconciler.Status().Update(t.Context(), f.source); err != nil {
							t.Fatal(err)
						}
					}
					member := DeriveISVC(f.source, "", "")
					member.Spec.Engine.Runner.Image = "standing-image"
					worker := f.workers["member-a"]
					if err := worker.Create(t.Context(), member); err != nil {
						t.Fatal(err)
					}
					before := member.DeepCopy()
					ctx, err := f.reconciler.preflightBackends(t.Context(), f.source, f.clusters, []string{"member-a", "member-b"})
					if err != nil {
						t.Fatal(err)
					}
					if tt.race {
						changed := false
						cl := interceptor.NewClient(worker, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
							if err := c.Get(ctx, key, obj, opts...); err != nil {
								return err
							}
							if _, runtimeRead := obj.(*v1beta1.ClusterServingRuntime); runtimeRead && !changed {
								changed = true
								tt.edit(t, f)
							}
							return nil
						}})
						f.connections.m["member-a"] = workloadcluster.NewNeverCachingClient(cl)
					} else {
						tt.edit(t, f)
					}
					if planned {
						err = f.reconciler.placePlannedOn(ctx, f.source, f.source.Status.Placement.Candidates[0])
					} else {
						err = f.reconciler.placeOn(ctx, "member-a", f.source, false)
					}
					if err == nil {
						t.Fatal("changed inputs authorized a member mutation")
					}
					got := &v1beta1.InferenceService{}
					if err := worker.Get(t.Context(), client.ObjectKeyFromObject(member), got); err != nil {
						t.Fatal(err)
					}
					if diff := cmp.Diff(before, got); diff != "" {
						t.Fatalf("held member (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}

func TestLocalServicesIgnoreSourceBackendPreflight(t *testing.T) {
	for _, mode := range []constants.DeploymentModeType{constants.OMENative, constants.RawDeployment, constants.MultiNode, constants.PDDisaggregated, constants.VirtualDeployment} {
		t.Run(string(mode), func(t *testing.T) {
			f := newBackendFixture(t, v1beta1.PlacementModeSingle)
			f.source.Spec.Placement = nil
			f.source.Spec.DeploymentMode = ptr.To(mode)
			f.source.Spec.Runtime.Name = "missing-runtime"
			if err := f.reconciler.Update(t.Context(), f.source); err != nil {
				t.Fatal(err)
			}
			before := f.source.DeepCopy()
			f.reconciler.Clusters = nil
			got := f.reconcile(t)
			if diff := cmp.Diff(before, got); diff != "" {
				t.Fatalf("local service mutated (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBackendHoldPreservesAcceptedAllocation(t *testing.T) {
	for _, mode := range []v1beta1.PlacementMode{v1beta1.PlacementModeSplit, v1beta1.PlacementModeSplitByCapacity} {
		t.Run(string(mode), func(t *testing.T) {
			f := newBackendFixture(t, mode)
			f.source.Status.Placement = plannedTestSource().Status.Placement
			if err := f.reconciler.Status().Update(t.Context(), f.source); err != nil {
				t.Fatal(err)
			}
			member := DeriveISVC(f.source, "", "")
			member.Spec.Engine.MinReplicas = ptr.To(2)
			worker := f.workers["member-a"]
			if err := worker.Create(t.Context(), member); err != nil {
				t.Fatal(err)
			}
			before := member.DeepCopy()
			accepted := f.source.Status.Placement.DeepCopy()
			f.source.Spec.Placement.Split.Replicas = ptr.To[int32](4)
			f.source.Generation++
			if err := f.reconciler.Update(t.Context(), f.source); err != nil {
				t.Fatal(err)
			}
			for name := range f.workers {
				f.runtimeMode(t, name, constants.RawDeployment)
			}
			got := f.reconcile(t)
			if diff := cmp.Diff(accepted.Plan, got.Status.Placement.Plan); diff != "" {
				t.Fatalf("accepted plan (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(accepted.Candidates[0].Allocation, got.Status.Placement.Candidates[0].Allocation); diff != "" {
				t.Fatalf("accepted floor (-want +got):\n%s", diff)
			}
			stored := &v1beta1.InferenceService{}
			if err := worker.Get(t.Context(), client.ObjectKeyFromObject(member), stored); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(before, stored); diff != "" {
				t.Fatalf("held member (-want +got):\n%s", diff)
			}
		})
	}
}

func TestStickyBackendRefreshRespectsMemberAbsence(t *testing.T) {
	for _, tt := range []struct {
		name    string
		present bool
		phase   v1beta1.PlacementPhase
	}{
		{name: "present winner refreshes", present: true, phase: v1beta1.PlacementPhasePlaced},
		{name: "absent winner enters grace", phase: v1beta1.PlacementPhasePending},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newBackendFixture(t, v1beta1.PlacementModeSingle)
			candidate := v1beta1.CandidatePlacement{Cluster: "member-a", Phase: v1beta1.CandidatePhaseAdmitted}
			f.source.Status.Placement = &v1beta1.PlacementStatus{Cluster: "member-a", Phase: v1beta1.PlacementPhasePlaced, Candidates: []v1beta1.CandidatePlacement{candidate}}
			if err := f.reconciler.Status().Update(t.Context(), f.source); err != nil {
				t.Fatal(err)
			}
			if tt.present {
				if err := f.workers["member-a"].Create(t.Context(), DeriveISVC(f.source, "", "")); err != nil {
					t.Fatal(err)
				}
				ir := irWithInstances(v1beta1.EngineComponent, true)
				ir.Namespace, ir.Name = f.source.Namespace, f.source.Name+"-engine"
				if err := f.workers["member-a"].Create(t.Context(), ir); err != nil {
					t.Fatal(err)
				}
			}
			ctx, err := f.reconciler.preflightBackends(t.Context(), f.source, f.clusters, []string{"member-a", "member-b"})
			if err != nil {
				t.Fatal(err)
			}
			observations := newPlacementObservations(f.source, f.clusters)
			observations.homes["member-a"] = homeObservation{state: homePresent, candidate: candidate}
			if _, err := f.reconciler.reconcileSinglePlanned(ctx, f.source, f.clusters, []string{"member-a", "member-b"}, observations); err != nil {
				t.Fatal(err)
			}
			got := &v1beta1.InferenceService{}
			key := client.ObjectKeyFromObject(f.source)
			if err := f.reconciler.Get(t.Context(), key, got); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.phase, got.Status.Placement.Phase); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff("member-a", got.Status.Placement.Cluster); diff != "" {
				t.Fatal(diff)
			}
			err = f.workers["member-a"].Get(t.Context(), key, &v1beta1.InferenceService{})
			if diff := cmp.Diff(!tt.present, apierrors.IsNotFound(err)); diff != "" {
				t.Fatalf("winner absence (-want +got):\n%s: %v", diff, err)
			}
			if !tt.present {
				for _, candidate := range got.Status.Placement.Candidates {
					if candidate.ReadyReplicas != 0 || candidate.AdmittedReplicas != 0 || candidate.Endpoint != nil {
						t.Fatal("absent winner retained admission evidence")
					}
				}
			}
		})
	}
}

func TestBackendHoldUsesCommittedWinner(t *testing.T) {
	scheme := testScheme(t)
	source := srcISVC("")
	source.Status.Placement = &v1beta1.PlacementStatus{Cluster: "a", Phase: v1beta1.PlacementPhasePlaced, Candidates: []v1beta1.CandidatePlacement{{Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted}}}
	winner := workerWithAdmittedDerived(t, scheme)
	runtime := backendTestRuntime()
	if err := winner.Get(t.Context(), client.ObjectKeyFromObject(runtime), runtime); err != nil {
		t.Fatal(err)
	}
	runtime.Spec.EngineConfig = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}}}
	if err := winner.Update(t.Context(), runtime); err != nil {
		t.Fatal(err)
	}
	other := emptyWorker(scheme)
	connections := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"a": workloadcluster.NewNeverCachingClient(winner), "b": workloadcluster.NewNeverCachingClient(other)}}
	r, root := newPlacer(scheme, connections, source, readyWC("a", nil), readyWC("b", nil))
	r.Client = interceptor.NewClient(root.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if err := cl.Get(ctx, key, obj, opts...); err != nil {
			return err
		}
		if service, ok := obj.(*v1beta1.InferenceService); ok {
			service.Status.Placement = nil
		}
		return nil
	}})
	if _, err := r.Reconcile(t.Context(), req()); err != nil {
		t.Fatal(err)
	}
	got := &v1beta1.InferenceService{}
	if err := root.Get(t.Context(), client.ObjectKeyFromObject(source), got); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff("a", got.Status.Placement.Cluster); diff != "" {
		t.Fatalf("committed winner (-want +got):\n%s", diff)
	}
	if err := winner.Get(t.Context(), client.ObjectKeyFromObject(source), &v1beta1.InferenceService{}); err != nil {
		t.Fatal(err)
	}
	if err := other.Get(t.Context(), client.ObjectKeyFromObject(source), &v1beta1.InferenceService{}); !apierrors.IsNotFound(err) {
		t.Fatalf("winner hold started another race: %v", err)
	}
}
