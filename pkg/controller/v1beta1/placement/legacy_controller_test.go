package placement

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

func legacySource(mode v1beta1.PlacementMode) *v1beta1.InferenceService {
	source := srcISVC("")
	source.Spec.Placement = &v1beta1.PlacementSpec{Mode: mode, Requirements: "accelerator=gpu-a"}
	source.Spec.Engine.MinReplicas = ptr.To(3)
	source.Spec.Engine.MaxReplicas = 7
	return source
}

func TestLegacyAllocationAndSpecUpdates(t *testing.T) {
	for _, tt := range []struct {
		name            string
		mode            v1beta1.PlacementMode
		annotation      bool
		spread          bool
		minimum         int32
		admitted        [2]int32
		want            map[string]int
		wantAfterUpdate map[string]int
	}{
		{name: "annotation Single", annotation: true, admitted: [2]int32{3, 0}, want: map[string]int{"a": 3}},
		{name: "implicit Single", admitted: [2]int32{3, 0}, want: map[string]int{"a": 3}},
		{name: "explicit Single", mode: v1beta1.PlacementModeSingle, admitted: [2]int32{3, 0}, want: map[string]int{"a": 3}},
		{name: "All", mode: v1beta1.PlacementModeAll, admitted: [2]int32{3, 3}, want: map[string]int{"a": 3, "b": 3}},
		{name: "Packed trims surplus", mode: v1beta1.PlacementModeSplit, admitted: [2]int32{3, 2}, want: map[string]int{"a": 3}},
		{name: "Packed fills deficit", mode: v1beta1.PlacementModeSplit, admitted: [2]int32{1, 1}, want: map[string]int{"a": 3, "b": 2}},
		{name: "spread retains ceiling rounding", mode: v1beta1.PlacementModeSplit, spread: true, admitted: [2]int32{2, 1}, want: map[string]int{"a": 2, "b": 2}},
		{name: "anti-sliver removes small home", mode: v1beta1.PlacementModeSplit, minimum: 2, admitted: [2]int32{2, 1}, want: map[string]int{"a": 3}, wantAfterUpdate: map[string]int{"a": 3, "b": 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			scheme := testScheme(t)
			source := legacySource(tt.mode)
			if tt.annotation {
				source.Spec.Placement = nil
				source.Annotations[AcceleratorRequirementsAnnotation] = "accelerator=gpu-a"
			}
			if tt.mode == v1beta1.PlacementModeSplit {
				source.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](3), Spread: tt.spread, MinReplicasPerCluster: tt.minimum, MaxReplicasPerCluster: 7}
			}
			workers := map[string]client.WithWatch{}
			peers := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}
			for i, name := range []string{"a", "b"} {
				workers[name] = workerWithReplicaCounts(t, scheme, "svc."+name+".example.com", tt.admitted[i], tt.admitted[i])
				peers.m[name] = workloadcluster.NewNeverCachingClient(workers[name])
			}
			r, cp := newPlacer(scheme, peers, source, readyWC("a", map[string]string{"accelerator": "gpu-a"}), readyWC("b", map[string]string{"accelerator": "gpu-a"}))
			for _, image := range []string{"image-a", "image-b"} {
				live := &v1beta1.InferenceService{}
				if err := cp.Get(t.Context(), client.ObjectKeyFromObject(source), live); err != nil {
					t.Fatal(err)
				}
				live.Spec.Engine.Runner.Container.Image = image
				live.Generation++
				if err := cp.Update(t.Context(), live); err != nil {
					t.Fatal(err)
				}
				if _, err := r.Reconcile(t.Context(), req()); err != nil {
					t.Fatal(err)
				}
				got := map[string]int{}
				for name, worker := range workers {
					derived := &v1beta1.InferenceService{}
					err := worker.Get(t.Context(), client.ObjectKeyFromObject(source), derived)
					if apierrors.IsNotFound(err) {
						continue
					}
					if err != nil {
						t.Fatal(err)
					}
					got[name] = *derived.Spec.Engine.MinReplicas
					if diff := cmp.Diff(image, derived.Spec.Engine.Runner.Container.Image); diff != "" {
						t.Errorf("member %s image (-want +got):\n%s", name, diff)
					}
					if diff := cmp.Diff(7, derived.Spec.Engine.MaxReplicas); diff != "" {
						t.Errorf("member %s ceiling (-want +got):\n%s", name, diff)
					}
					policy, err := protocol.FromDerived(derived)
					if err != nil {
						t.Fatal(err)
					}
					if policy != nil {
						t.Fatal("legacy member acquired execution policy")
					}
					if derived.Spec.Placement != nil {
						t.Fatal("derived copy retained source placement")
					}
				}
				want := tt.want
				if image == "image-b" && tt.wantAfterUpdate != nil {
					want = tt.wantAfterUpdate
				}
				if diff := cmp.Diff(want, got); diff != "" {
					t.Fatalf("member floors (-want +got):\n%s", diff)
				}
				if err := cp.Get(t.Context(), client.ObjectKeyFromObject(source), live); err != nil {
					t.Fatal(err)
				}
				if live.Status.Placement.Plan != nil {
					t.Fatal("legacy source acquired plan")
				}
			}
		})
	}
}

func TestLegacySingleRetainsAndReplacesWinner(t *testing.T) {
	for _, annotation := range []bool{false, true} {
		name := "typed"
		if annotation {
			name = "annotation"
		}
		t.Run(name, func(t *testing.T) {
			scheme := testScheme(t)
			source := legacySource(v1beta1.PlacementModeSingle)
			if annotation {
				source.Spec.Placement = nil
				source.Annotations[AcceleratorRequirementsAnnotation] = "accelerator=gpu-a"
			}
			source.Finalizers = []string{PlacementFinalizer}
			source.Status.Placement = &v1beta1.PlacementStatus{Cluster: "b", Phase: v1beta1.PlacementPhasePlaced, Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted}}}
			a, b := emptyWorker(scheme), workerWithReplicaCounts(t, scheme, "svc.b.example.com", 3, 3)
			r, cp := newPlacer(scheme, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"a": workloadcluster.NewNeverCachingClient(a), "b": workloadcluster.NewNeverCachingClient(b)}}, source, readyWC("a", map[string]string{"accelerator": "gpu-a"}), readyWC("b", map[string]string{"accelerator": "gpu-a"}))
			if _, err := r.Reconcile(t.Context(), req()); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff("b", cpPlacement(t, cp).Cluster); diff != "" {
				t.Fatal(diff)
			}
			if hasDerived(t, a) {
				t.Fatal("sticky winner caused fanout")
			}
			derived := &v1beta1.InferenceService{}
			if err := b.Get(t.Context(), client.ObjectKeyFromObject(source), derived); err != nil {
				t.Fatal(err)
			}
			if err := b.Delete(t.Context(), derived); err != nil {
				t.Fatal(err)
			}
			r.winnerLostSince.Store(source.UID, time.Now().Add(-2*r.winnerLostGrace()))
			replacement := workerWithReplicaCounts(t, scheme, "svc.a.example.com", 3, 3)
			r.Clusters = fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"a": workloadcluster.NewNeverCachingClient(replacement), "b": workloadcluster.NewNeverCachingClient(b)}}
			if _, err := r.Reconcile(t.Context(), req()); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff("a", cpPlacement(t, cp).Cluster); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestLegacyMutationsRespectPolicyHandoff(t *testing.T) {
	for _, tt := range []struct {
		name                        string
		changeSource, plannedMember bool
	}{
		{name: "unchanged legacy"},
		{name: "source opted in", changeSource: true},
		{name: "member has plan", plannedMember: true},
	} {
		for _, action := range []string{"apply", "delete"} {
			t.Run(tt.name+"/"+action, func(t *testing.T) {
				scheme := testScheme(t)
				source := legacySource(v1beta1.PlacementModeSingle)
				source.Finalizers = []string{PlacementFinalizer}
				worker := emptyWorker(scheme)
				derived := DeriveISVC(source, "", "")
				derived.UID = types.UID("derived-uid")
				if tt.plannedMember {
					policy := &v1beta1.PlacementExecutionPolicy{SourceUID: source.UID, ClusterUID: "cluster-uid", PlanID: "plan", Revision: 1}
					encoded, err := protocol.Encode(policy)
					if err != nil {
						t.Fatal(err)
					}
					if derived.Annotations == nil {
						derived.Annotations = map[string]string{}
					}
					derived.Annotations[constants.PlacementExecution] = encoded
				}
				if err := worker.Create(t.Context(), derived); err != nil {
					t.Fatal(err)
				}
				r, cp := newPlacer(scheme, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"a": workloadcluster.NewNeverCachingClient(worker)}}, source)
				if tt.changeSource {
					live := source.DeepCopy()
					live.Spec.Placement = &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle}
					live.Generation++
					if err := cp.Update(t.Context(), live); err != nil {
						t.Fatal(err)
					}
				}
				var err error
				if action == "apply" {
					err = r.legacyPlaceOn(t.Context(), "a", worker, source)
				} else {
					err = r.legacyDeleteDerivedOn(t.Context(), "a", source)
				}
				if diff := cmp.Diff(tt.changeSource || tt.plannedMember, err != nil); diff != "" {
					t.Fatalf("mutation error (-want +got):\n%s; %v", diff, err)
				}
				if tt.changeSource || tt.plannedMember {
					got := &v1beta1.InferenceService{}
					if err := worker.Get(t.Context(), client.ObjectKeyFromObject(derived), got); err != nil {
						t.Fatal(err)
					}
					if diff := cmp.Diff(derived, got); diff != "" {
						t.Fatal(diff)
					}
				}
			})
		}
	}
}

func TestLegacyUnknownHomesRetainAllocation(t *testing.T) {
	for _, tt := range []struct {
		name            string
		mode            v1beta1.PlacementMode
		registeredReady bool
		wantFloor       int
		wantUnchanged   bool
	}{
		{name: "Single retains unknown winner", mode: v1beta1.PlacementModeSingle, registeredReady: true, wantUnchanged: true},
		{name: "All continues healthy home", mode: v1beta1.PlacementModeAll, registeredReady: true, wantFloor: 3},
		{name: "Packed retains unknown admission", mode: v1beta1.PlacementModeSplit, registeredReady: true, wantFloor: 5},
		{name: "Packed reserves unready home", mode: v1beta1.PlacementModeSplit, wantFloor: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			scheme := testScheme(t)
			source := legacySource(tt.mode)
			if tt.mode == v1beta1.PlacementModeSplit {
				source.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](5)}
			}
			source.Status.Placement = &v1beta1.PlacementStatus{Phase: v1beta1.PlacementPhasePlaced, Candidates: []v1beta1.CandidatePlacement{{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted, AdmittedReplicas: 2, ReadyReplicas: 2}}}
			if tt.mode == v1beta1.PlacementModeSingle {
				source.Status.Placement.Cluster = "b"
			}
			good := workerWithReplicaCounts(t, scheme, "a.example.com", 1, 1)
			badBase := workerWithReplicaCounts(t, scheme, "b.example.com", 2, 2)
			bad := irGetFailClient{WithWatch: badBase}
			registration := readyWC("b", map[string]string{"accelerator": "gpu-a"})
			if !tt.registeredReady {
				registration.Status.Conditions[0].Status = "False"
			}
			r, cp := newPlacer(scheme, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"a": workloadcluster.NewNeverCachingClient(good), "b": workloadcluster.NewNeverCachingClient(bad)}}, source, readyWC("a", map[string]string{"accelerator": "gpu-a"}), registration)
			before := &v1beta1.InferenceService{}
			if err := badBase.Get(t.Context(), client.ObjectKeyFromObject(source), before); err != nil {
				t.Fatal(err)
			}
			goodBefore := &v1beta1.InferenceService{}
			if err := good.Get(t.Context(), client.ObjectKeyFromObject(source), goodBefore); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Reconcile(t.Context(), req()); err != nil {
				t.Fatal(err)
			}
			after := &v1beta1.InferenceService{}
			if err := badBase.Get(t.Context(), client.ObjectKeyFromObject(source), after); err != nil {
				t.Fatal(err)
			}
			if tt.mode != v1beta1.PlacementModeAll {
				if diff := cmp.Diff(before, after); diff != "" {
					t.Fatalf("unknown member changed:\n%s", diff)
				}
			} else if diff := cmp.Diff(3, *after.Spec.Engine.MinReplicas); diff != "" {
				t.Fatal(diff)
			}
			if err := good.Get(t.Context(), client.ObjectKeyFromObject(source), after); err != nil {
				t.Fatal(err)
			}
			if tt.wantUnchanged {
				if diff := cmp.Diff(goodBefore, after); diff != "" {
					t.Errorf("healthy member changed (-want +got):\n%s", diff)
				}
			} else if diff := cmp.Diff(ptr.To(tt.wantFloor), after.Spec.Engine.MinReplicas); diff != "" {
				t.Errorf("healthy floor (-want +got):\n%s", diff)
			}
			candidates := candidatesByCluster(cpPlacement(t, cp).Candidates)
			home, exists := candidates["b"]
			if !exists {
				t.Fatal("unknown home lost from status")
			}
			if diff := cmp.Diff([2]int32{2, 0}, [2]int32{home.AdmittedReplicas, home.ReadyReplicas}); diff != "" {
				t.Errorf("unknown counts (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLegacySplitTargets(t *testing.T) {
	for _, tt := range []struct {
		name           string
		admitted       map[string]int32
		floor, ceiling int32
		spread         bool
		want           map[string]int32
	}{
		{name: "Packed over-requests unadmitted deficit", admitted: map[string]int32{}, floor: 5, want: map[string]int32{"a": 5, "b": 5}},
		{name: "Packed trims excess in preference order", admitted: map[string]int32{"a": 4, "b": 3}, floor: 5, want: map[string]int32{"a": 4, "b": 1}},
		{name: "Packed clips to local ceiling", admitted: map[string]int32{}, floor: 5, ceiling: 2, want: map[string]int32{"a": 2, "b": 2}},
		{name: "spread clips to local ceiling", floor: 5, ceiling: 2, spread: true, want: map[string]int32{"a": 2, "b": 2}},
		{name: "spread rounds each home up", floor: 5, spread: true, want: map[string]int32{"a": 3, "b": 3}},
		{name: "no remaining floor", floor: 0, want: map[string]int32{"a": 0, "b": 0}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := legacySplitApportion([]string{"a", "b"}, tt.admitted, tt.floor, tt.ceiling, tt.spread)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestLegacyComponentReplicaBands(t *testing.T) {
	for _, tt := range []struct {
		name                      string
		maximum, ceiling, wantMax int
	}{
		{name: "inherit existing local ceiling", maximum: 7, wantMax: 7},
		{name: "raise a ceiling below the requested floor", maximum: 1, wantMax: 3},
		{name: "unset component ceiling follows legacy floor", wantMax: 3},
		{name: "explicit split ceiling", maximum: 7, ceiling: 4, wantMax: 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			service := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1), MaxReplicas: tt.maximum}},
				Decoder: &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1), MaxReplicas: tt.maximum}},
				Router:  &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1), MaxReplicas: 2}},
			}}
			legacySetDerivedReplicas(service, 3, int32(tt.ceiling))
			got := [6]int{*service.Spec.Engine.MinReplicas, service.Spec.Engine.MaxReplicas, *service.Spec.Decoder.MinReplicas, service.Spec.Decoder.MaxReplicas, *service.Spec.Router.MinReplicas, service.Spec.Router.MaxReplicas}
			if diff := cmp.Diff([6]int{3, tt.wantMax, 3, tt.wantMax, 1, 2}, got); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
