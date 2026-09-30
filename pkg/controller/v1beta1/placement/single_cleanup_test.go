package placement

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

func TestSingleRaceCleanupAuthority(t *testing.T) {
	for _, tt := range []struct {
		name                 string
		edit                 func(*v1beta1.InferenceService, *v1beta1.CandidatePlacement, *v1beta1.InferenceService)
		stale, race, wantErr bool
	}{
		{name: "committed race loser can be removed"},
		{name: "pending race cannot delete", wantErr: true, edit: func(s *v1beta1.InferenceService, _ *v1beta1.CandidatePlacement, _ *v1beta1.InferenceService) {
			s.Status.Placement.Plan.Winner = ""
		}},
		{name: "winner cannot be removed", wantErr: true, edit: func(s *v1beta1.InferenceService, _ *v1beta1.CandidatePlacement, _ *v1beta1.InferenceService) {
			s.Status.Placement.Plan.Winner = "member-a"
		}},
		{name: "retained home has no race exception", wantErr: true, edit: func(s *v1beta1.InferenceService, c *v1beta1.CandidatePlacement, _ *v1beta1.InferenceService) {
			c.Allocation.RaceCandidate = false
			s.Status.Placement.Candidates[0] = *c.DeepCopy()
		}},
		{name: "detached race permission cannot delete", wantErr: true, edit: func(s *v1beta1.InferenceService, _ *v1beta1.CandidatePlacement, _ *v1beta1.InferenceService) {
			s.Status.Placement.Candidates[0].Allocation.RaceCandidate = false
		}},
		{name: "desired home is retained", wantErr: true, edit: func(s *v1beta1.InferenceService, c *v1beta1.CandidatePlacement, _ *v1beta1.InferenceService) {
			c.Allocation.DesiredReplicas = 1
			s.Status.Placement.Candidates[0] = *c.DeepCopy()
		}},
		{name: "other mode cannot use race cleanup", wantErr: true, edit: func(s *v1beta1.InferenceService, _ *v1beta1.CandidatePlacement, _ *v1beta1.InferenceService) {
			s.Status.Placement.Plan.Mode = v1beta1.PlacementModeAll
		}},
		{name: "ordinary local collision is retained", wantErr: true, edit: func(_ *v1beta1.InferenceService, _ *v1beta1.CandidatePlacement, m *v1beta1.InferenceService) {
			m.Labels, m.Annotations = nil, nil
		}},
		{name: "new source authority fences old cleanup", stale: true, wantErr: true},
		{name: "concurrent member edit fences deletion", race: true, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := plannedTestSource()
			s.Spec.Placement.Mode = v1beta1.PlacementModeSingle
			s.Status.Placement.Plan.Mode = v1beta1.PlacementModeSingle
			s.Status.Placement.Plan.Winner = "member-b"
			a := s.Status.Placement.Candidates[0].Allocation
			a.CurrentReplicas, a.DesiredReplicas = 0, 0
			a.RaceCandidate, a.DrainRequested = true, true
			candidate := s.Status.Placement.Candidates[0].DeepCopy()
			member := DeriveISVC(s, "", "")
			member.UID = "member-service-uid"
			if tt.edit != nil {
				tt.edit(s, candidate, member)
			}
			worker := emptyWorker(testScheme(t))
			if err := worker.Create(t.Context(), member); err != nil {
				t.Fatal(err)
			}
			watched := interceptor.NewClient(worker, interceptor.Funcs{Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if tt.race {
					changed := &v1beta1.InferenceService{}
					if err := cl.Get(ctx, client.ObjectKeyFromObject(obj), changed); err != nil {
						return err
					}
					changed.Annotations["example.com/member-edit"] = "changed"
					if err := cl.Update(ctx, changed); err != nil {
						return err
					}
				}
				return cl.Delete(ctx, obj, opts...)
			}})
			live := s.DeepCopy()
			if tt.stale {
				live.Status.Placement.Plan.Revision++
			}
			connections := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(watched)}}
			r, _ := newPlacer(testScheme(t), connections, live, plannedTestRegistration())
			err := r.deletePlannedRaceLoser(t.Context(), s, *candidate)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("error %v: %s", err, diff)
			}
			err = worker.Get(t.Context(), client.ObjectKeyFromObject(member), &v1beta1.InferenceService{})
			if diff := cmp.Diff(!tt.wantErr, apierrors.IsNotFound(err)); diff != "" {
				t.Fatalf("member presence %v: %s", err, diff)
			}
		})
	}
}
