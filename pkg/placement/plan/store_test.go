package plan

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func sourceFixture() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "team-a", UID: "source-a", Generation: 1},
		Spec:       v1beta1.InferenceServiceSpec{Placement: &v1beta1.PlacementSpec{Mode: v1beta1.PlacementModeSplit}},
	}
}

func proposalFixture() Proposal {
	return Proposal{InputDigest: "intent-a", Assignments: map[string]v1beta1.CandidateAllocationStatus{
		"member-b": {ClusterUID: "registration-b", OriginalReplicas: 0, CurrentReplicas: 1, DesiredReplicas: 3, MatchingTerms: []int32{2, 0}, Weight: ptr.To(int64(1))},
		"member-a": {ClusterUID: "registration-a", OriginalReplicas: 6, CurrentReplicas: 6, DesiredReplicas: 3, Weight: ptr.To(int64(1))},
	}}
}

func capacityFixture() *v1beta1.PlacementCapacitySample {
	return &v1beta1.PlacementCapacitySample{DemandFingerprint: "demand-a", Replicas: 4, Pools: []v1beta1.PlacementCapacityPool{{
		ResourceName: "nvidia.com/gpu", ResourceFlavor: "gpu-a", Demand: 2, Allocatable: 8,
		ObservedAt: metav1.NewTime(time.Unix(100, 0).UTC()), ReportUID: "report-a", ReportResourceVersion: "1",
		Attribution: v1beta1.AcceleratorCapacityAttribution{FlavorUID: "flavor-a", FlavorSetHash: "mapping-a", Complete: true},
	}}}
}

func storeFixture(t *testing.T, source *v1beta1.InferenceService, hooks interceptor.Funcs) Store {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1beta1.InferenceService{}).WithObjects(source).WithInterceptorFuncs(hooks).Build()
	return Store{Client: cl, Reader: cl}
}

func mustPersist(t *testing.T, store Store, source *v1beta1.InferenceService, proposal Proposal) *v1beta1.InferenceService {
	t.Helper()
	got, err := store.Persist(t.Context(), source, proposal)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestPersistPlanAcrossRestartAndRetarget(t *testing.T) {
	source := sourceFixture()
	store := storeFixture(t, source, interceptor.Funcs{})
	proposal := proposalFixture()
	first := mustPersist(t, store, source, proposal)
	want := &v1beta1.PlacementPlanStatus{ID: first.Status.Placement.Plan.ID, Revision: 1, SourceUID: source.UID, ObservedGeneration: 1, InputDigest: "intent-a", RequestedReplicas: 6, AssignedReplicas: 6}
	if diff := cmp.Diff(want, first.Status.Placement.Plan); diff != "" {
		t.Fatalf("plan (-want +got):\n%s", diff)
	}
	if first.Status.Placement.Plan.ID == "" {
		t.Fatal("missing plan ID")
	}
	seen := map[string]bool{first.Status.Placement.Plan.ID: true}
	for _, tt := range []struct {
		name         string
		mutate       func(*Proposal)
		wantRevision int64
	}{
		{name: "restart with identical input", wantRevision: 1},
		{name: "step changes current floor", wantRevision: 2, mutate: func(p *Proposal) {
			a := p.Assignments["member-b"]
			a.CurrentReplicas = 2
			p.Assignments["member-b"] = a
		}},
		{name: "retarget preserves original floor", wantRevision: 3, mutate: func(p *Proposal) {
			a := p.Assignments["member-b"]
			a.DesiredReplicas = 4
			p.Assignments["member-b"] = a
		}},
		{name: "drain instruction gets authority", wantRevision: 4, mutate: func(p *Proposal) {
			a := p.Assignments["member-a"]
			a.DrainRequested = true
			p.Assignments["member-a"] = a
		}},
		{name: "return to earlier allocation", wantRevision: 5, mutate: func(p *Proposal) { *p = proposalFixture() }},
		{name: "pause acquires new authority", wantRevision: 6, mutate: func(p *Proposal) { p.PauseSurge = true }},
		{name: "release acquires new authority", wantRevision: 7, mutate: func(p *Proposal) { p.PauseSurge = false }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mutate != nil {
				tt.mutate(&proposal)
			}
			got := mustPersist(t, Store{Client: store.Client, Reader: store.Reader}, first, proposal)
			if diff := cmp.Diff(tt.wantRevision, got.Status.Placement.Plan.Revision); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(int32(6), got.Status.Placement.Candidates[0].Allocation.OriginalReplicas); diff != "" {
				t.Fatal(diff)
			}
			if tt.wantRevision == 1 {
				if diff := cmp.Diff(first, got); diff != "" {
					t.Fatalf("idempotent retry wrote status:\n%s", diff)
				}
			} else if seen[got.Status.Placement.Plan.ID] {
				t.Fatal("changed allocation reused authority")
			}
			seen[got.Status.Placement.Plan.ID] = true
			first = got
		})
	}
}

func TestPersistFencesConcurrentChanges(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*v1beta1.InferenceService)
		want   error
	}{
		{name: "source recreation", mutate: func(s *v1beta1.InferenceService) { s.UID = "source-b" }, want: ErrStaleSnapshot},
		{name: "intent annotation changes", mutate: func(s *v1beta1.InferenceService) {
			s.Annotations = map[string]string{"example.com/runtime-sync": "next"}
		}, want: ErrStaleSnapshot},
		{name: "source label changes", mutate: func(s *v1beta1.InferenceService) { s.Labels = map[string]string{"app": "updated"} }, want: ErrStaleSnapshot},
		{name: "new source generation", mutate: func(s *v1beta1.InferenceService) { s.Generation++ }, want: ErrStaleSnapshot},
		{name: "new plan revision", mutate: func(s *v1beta1.InferenceService) { s.Status.Placement.Plan.Revision++ }, want: ErrStaleSnapshot},
		{name: "new plan identity", mutate: func(s *v1beta1.InferenceService) { s.Status.Placement.Plan.ID = "different" }, want: ErrStaleSnapshot},
		{name: "deletion in progress", mutate: func(s *v1beta1.InferenceService) {
			s.DeletionTimestamp = ptr.To(metav1.Now())
			s.Finalizers = []string{"example.com/retain"}
		}, want: ErrStaleSnapshot},
		{name: "unrelated status writer", mutate: func(s *v1beta1.InferenceService) { s.Status.Placement.Phase = v1beta1.PlacementPhasePlaced }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := sourceFixture()
			initial := storeFixture(t, source, interceptor.Funcs{})
			source = mustPersist(t, initial, source, proposalFixture())
			current := source.DeepCopy()
			tt.mutate(current)
			store := storeFixture(t, current, interceptor.Funcs{})
			got, err := store.Persist(t.Context(), source, proposalFixture())
			if !errors.Is(err, tt.want) {
				t.Fatalf("want %v, got %v", tt.want, err)
			}
			if tt.want != nil && got != nil {
				t.Fatal("stale plan authorized member writes")
			}
			if tt.want == nil {
				if diff := cmp.Diff(current.Status, got.Status); diff != "" {
					t.Fatal(diff)
				}
			}
		})
	}
}

func TestPersistConflictAndReadback(t *testing.T) {
	failure := errors.New("API unavailable")
	for _, tt := range []struct {
		name       string
		change     func(*v1beta1.InferenceService)
		conflict   bool
		writeError bool
		readError  bool
		want       error
		wantWrites int
	}{
		{name: "status contention", conflict: true, wantWrites: 2},
		{name: "intent changes during conflict", conflict: true, change: func(s *v1beta1.InferenceService) { s.Generation++ }, want: ErrStaleSnapshot, wantWrites: 1},
		{name: "whole plan pruned", change: func(s *v1beta1.InferenceService) { s.Status.Placement.Plan = nil }, want: ErrSchemaUnsupported, wantWrites: 1},
		{name: "assignment pruned", change: func(s *v1beta1.InferenceService) { s.Status.Placement.Candidates[0].Allocation = nil }, want: ErrSchemaUnsupported, wantWrites: 1},
		{name: "partial assignment pruned", change: func(s *v1beta1.InferenceService) { s.Status.Placement.Candidates[0].Allocation.CurrentReplicas = 0 }, want: ErrSchemaUnsupported, wantWrites: 1},
		{name: "new plan before readback", change: func(s *v1beta1.InferenceService) { s.Status.Placement.Plan.Revision++ }, want: ErrStaleSnapshot, wantWrites: 1},
		{name: "write failure", writeError: true, want: failure, wantWrites: 1},
		{name: "readback failure", readError: true, want: failure, wantWrites: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			writes, reads := 0, 0
			hooks := interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					reads++
					if tt.readError && reads == 2 {
						return failure
					}
					if err := cl.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					if tt.change != nil && reads == 2 {
						tt.change(obj.(*v1beta1.InferenceService))
					}
					return nil
				},
				SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					writes++
					if tt.writeError {
						return failure
					}
					if tt.conflict && writes == 1 {
						return apierrors.NewConflict(schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}, obj.GetName(), failure)
					}
					return cl.SubResource(sub).Update(ctx, obj, opts...)
				},
			}
			source := sourceFixture()
			store := storeFixture(t, source, hooks)
			got, err := store.Persist(t.Context(), source, proposalFixture())
			if !errors.Is(err, tt.want) {
				t.Fatalf("want %v, got %v", tt.want, err)
			}
			if tt.want != nil && got != nil {
				t.Fatal("failed verification authorized member writes")
			}
			if diff := cmp.Diff(tt.wantWrites, writes); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestPersistPreservesObservationsByClusterIdentity(t *testing.T) {
	for _, tt := range []struct {
		name      string
		replace   bool
		wantKnown bool
	}{
		{name: "same incarnation", wantKnown: true}, {name: "replacement registration", replace: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := sourceFixture()
			store := storeFixture(t, source, interceptor.Funcs{})
			source = mustPersist(t, store, source, proposalFixture())
			source.Status.Placement.Candidates[0].ObservationKnown = true
			source.Status.Placement.Candidates[0].AppliedPlanID = source.Status.Placement.Plan.ID
			if err := store.Client.Status().Update(t.Context(), source); err != nil {
				t.Fatal(err)
			}
			proposal := proposalFixture()
			proposal.InputDigest = "intent-b"
			if tt.replace {
				a := proposal.Assignments["member-a"]
				a.ClusterUID = "registration-new"
				proposal.Assignments["member-a"] = a
			}
			got := mustPersist(t, store, source, proposal)
			if diff := cmp.Diff(tt.wantKnown, got.Status.Placement.Candidates[0].ObservationKnown); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestPersistRejectsInvalidStoreOrSource(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*Store, **v1beta1.InferenceService)
	}{
		{name: "missing client", mutate: func(s *Store, _ **v1beta1.InferenceService) { s.Client = nil }},
		{name: "missing direct reader", mutate: func(s *Store, _ **v1beta1.InferenceService) { s.Reader = nil }},
		{name: "nil source", mutate: func(_ *Store, s **v1beta1.InferenceService) { *s = nil }},
		{name: "unidentified source", mutate: func(_ *Store, s **v1beta1.InferenceService) { (*s).UID = "" }},
		{name: "derived label with typed intent", mutate: func(_ *Store, s **v1beta1.InferenceService) {
			(*s).Labels = map[string]string{constants.PlacementOrigin: "parent-a"}
		}},
		{name: "derived annotation with typed intent", mutate: func(_ *Store, s **v1beta1.InferenceService) {
			(*s).Annotations = map[string]string{constants.PlacementOriginUID: "parent-a"}
		}},
		{name: "local service", mutate: func(_ *Store, s **v1beta1.InferenceService) { (*s).Spec.Placement = nil }},
		{name: "revision exhausted", mutate: func(s *Store, source **v1beta1.InferenceService) {
			*source = mustPersist(t, *s, *source, proposalFixture())
			(*source).Status.Placement.Plan.Revision = math.MaxInt64
			if err := s.Client.Status().Update(t.Context(), *source); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := sourceFixture()
			store := storeFixture(t, source, interceptor.Funcs{})
			tt.mutate(&store, &source)
			proposal := proposalFixture()
			proposal.InputDigest = "intent-b"
			got, err := store.Persist(t.Context(), source, proposal)
			if err == nil || got != nil {
				t.Fatalf("want rejection, got %v, %v", got, err)
			}
		})
	}
}

func TestPersistRejectsDamagedAcceptedAuthority(t *testing.T) {
	for _, tt := range []struct {
		name   string
		damage func(*v1beta1.PlacementStatus)
	}{
		{name: "missing candidate", damage: func(s *v1beta1.PlacementStatus) { s.Candidates = s.Candidates[:1] }},
		{name: "duplicate candidate", damage: func(s *v1beta1.PlacementStatus) { s.Candidates[1] = s.Candidates[0] }},
		{name: "missing allocation", damage: func(s *v1beta1.PlacementStatus) { s.Candidates[0].Allocation = nil }},
		{name: "changed floor", damage: func(s *v1beta1.PlacementStatus) { s.Candidates[0].Allocation.OriginalReplicas = 0 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := sourceFixture()
			store := storeFixture(t, source, interceptor.Funcs{})
			source = mustPersist(t, store, source, proposalFixture())
			tt.damage(source.Status.Placement)
			if err := store.Client.Status().Update(t.Context(), source); err != nil {
				t.Fatal(err)
			}
			got, err := store.Persist(t.Context(), source, proposalFixture())
			if !errors.Is(err, ErrSchemaUnsupported) || got != nil {
				t.Fatalf("want unsupported authority, got %v, %v", got, err)
			}
		})
	}
}

func TestPersistEmptyAllocation(t *testing.T) {
	for _, tt := range []struct {
		name       string
		unassigned int32
	}{
		{name: "zero floor"}, {name: "all replicas unassigned", unassigned: 6},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := sourceFixture()
			store := storeFixture(t, source, interceptor.Funcs{})
			proposal := Proposal{InputDigest: "empty-match", UnassignedReplicas: tt.unassigned}
			got := mustPersist(t, store, source, proposal)
			if diff := cmp.Diff(int64(tt.unassigned), got.Status.Placement.Plan.RequestedReplicas); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(int64(0), got.Status.Placement.Plan.AssignedReplicas); diff != "" {
				t.Fatal(diff)
			}
			again := mustPersist(t, store, got, proposal)
			if diff := cmp.Diff(got, again); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
