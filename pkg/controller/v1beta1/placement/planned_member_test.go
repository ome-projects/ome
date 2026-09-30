package placement

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

type identifiedTestClusters struct {
	fakeClusters
	uid types.UID
}

func (c identifiedTestClusters) ClientForUID(name string, uid types.UID) (workloadcluster.SelectivelyCachingClient, bool) {
	if uid != c.uid {
		return nil, false
	}
	return c.ClientFor(name)
}

func plannedTestSource() *v1beta1.InferenceService {
	source := srcISVCSplit("", 2)
	source.Namespace = "team-a"
	source.Generation = 1
	source.Status.Placement = &v1beta1.PlacementStatus{
		Plan:       &v1beta1.PlacementPlanStatus{ID: "plan-a", Revision: 2, SourceUID: source.UID, ObservedGeneration: source.Generation},
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "member-a", Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "member-a-uid", CurrentReplicas: 2, DesiredReplicas: 2}}},
	}
	return source
}

func TestPlannedMemberAuthority(t *testing.T) {
	for _, tt := range []struct {
		name          string
		editSource    func(*v1beta1.InferenceService)
		editMember    func(*v1beta1.InferenceService)
		editSnapshot  func(*v1beta1.InferenceService)
		editCandidate func(*v1beta1.CandidatePlacement)
		clusterUID    types.UID
		legacy        bool
		noReader      bool
		disconnected  bool
		wantErr       bool
	}{
		{name: "current plan applies"},
		{name: "capacity plan needs persisted demand", editSnapshot: func(s *v1beta1.InferenceService) { s.Spec.Placement.Mode = v1beta1.PlacementModeSplitByCapacity }, wantErr: true},
		{name: "invalid persisted capacity cannot apply", editSnapshot: func(s *v1beta1.InferenceService) {
			attachCapacityContract(t, s)
			s.Status.Placement.Candidates[0].Allocation.Capacity.DemandContract = nil
		}, wantErr: true},
		{name: "detached current floor cannot apply", editCandidate: func(c *v1beta1.CandidatePlacement) { c.Allocation.CurrentReplicas++ }, wantErr: true},
		{name: "unplanned cluster cannot apply", editCandidate: func(c *v1beta1.CandidatePlacement) { c.Cluster = "member-b" }, wantErr: true},
		{name: "missing plan cannot apply", editSnapshot: func(s *v1beta1.InferenceService) { s.Status.Placement.Plan = nil }, wantErr: true},
		{name: "wrong source plan cannot apply", editSnapshot: func(s *v1beta1.InferenceService) { s.Status.Placement.Plan.SourceUID = "other-source" }, wantErr: true},
		{name: "stale plan generation cannot apply", editSnapshot: func(s *v1beta1.InferenceService) { s.Status.Placement.Plan.ObservedGeneration-- }, wantErr: true},
		{name: "zero floor cannot create a member", editSnapshot: func(s *v1beta1.InferenceService) { s.Status.Placement.Candidates[0].Allocation.CurrentReplicas = 0 }, wantErr: true},
		{name: "unidentified cluster cannot apply", editSnapshot: func(s *v1beta1.InferenceService) { s.Status.Placement.Candidates[0].Allocation.ClusterUID = "" }, wantErr: true},
		{name: "direct source reader is required", noReader: true, wantErr: true},
		{name: "disconnected cluster cannot apply", disconnected: true, wantErr: true},
		{name: "malformed member policy cannot be overwritten", editMember: func(s *v1beta1.InferenceService) { s.Annotations[constants.PlacementExecution] = "invalid" }, wantErr: true},
		{name: "source generation changed", editSource: func(s *v1beta1.InferenceService) { s.Generation++ }, wantErr: true},
		{name: "source plan changed", editSource: func(s *v1beta1.InferenceService) { s.Status.Placement.Plan.Revision++ }, wantErr: true},
		{name: "registration replaced", clusterUID: "replacement-a", wantErr: true},
		{name: "name-only transport rejected", legacy: true, wantErr: true},
		{name: "ordinary local collision", editMember: func(s *v1beta1.InferenceService) { s.Labels = nil; s.Annotations = nil }, wantErr: true},
		{name: "newer member policy", editMember: func(s *v1beta1.InferenceService) {
			p := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-b", Revision: 3, SourceUID: "uid-1", ClusterUID: "member-a-uid", PauseSurge: true}
			raw, _ := protocol.Encode(p)
			s.Annotations[constants.PlacementExecution] = raw
		}, wantErr: true},
		{name: "same revision other authority", editMember: func(s *v1beta1.InferenceService) {
			p := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-b", Revision: 2, SourceUID: "uid-1", ClusterUID: "member-a-uid", PauseSurge: true}
			raw, _ := protocol.Encode(p)
			s.Annotations[constants.PlacementExecution] = raw
		}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			scheme := testScheme(t)
			source := plannedTestSource()
			if tt.editSnapshot != nil {
				tt.editSnapshot(source)
			}
			live := source.DeepCopy()
			if tt.editSource != nil {
				tt.editSource(live)
			}
			registration := plannedTestRegistration()
			if tt.clusterUID != "" {
				registration.UID = tt.clusterUID
			}
			member := DeriveISVC(source, "", "")
			member.UID = "member-service-a"
			member.Annotations["example.com/member-metadata"] = "retained"
			if tt.editMember != nil {
				tt.editMember(member)
			}
			worker := emptyWorker(scheme)
			if err := worker.Create(ctx, member); err != nil {
				t.Fatal(err)
			}
			before := member.DeepCopy()
			base := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(worker)}}
			if tt.disconnected {
				base.m = nil
			}
			var clusters ClusterClients = identifiedTestClusters{fakeClusters: base, uid: "member-a-uid"}
			if tt.legacy {
				clusters = struct{ ClusterClients }{base}
			}
			r, _ := newPlacer(scheme, clusters, live, registration)
			if tt.noReader {
				r.APIReader = nil
			}
			candidate := source.Status.Placement.Candidates[0].DeepCopy()
			if tt.editCandidate != nil {
				tt.editCandidate(candidate)
			}
			err := r.placePlannedOn(ctx, source, *candidate)
			if (err != nil) != tt.wantErr {
				t.Fatalf("apply error = %v, want error %t", err, tt.wantErr)
			}
			stored := &v1beta1.InferenceService{}
			if err := worker.Get(ctx, client.ObjectKeyFromObject(member), stored); err != nil {
				t.Fatal(err)
			}
			if tt.wantErr {
				if diff := cmp.Diff(before, stored); diff != "" {
					t.Errorf("rejected apply mutated member (-want +got):\n%s", diff)
				}
				return
			}
			got, err := protocol.FromDerived(stored)
			if err != nil {
				t.Fatal(err)
			}
			want := executionPolicy(source, source.Status.Placement.Candidates[0].Allocation)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("policy (-want +got):\n%s", diff)
			}
			if stored.Spec.Placement != nil || stored.Spec.Engine.MinReplicas == nil || *stored.Spec.Engine.MinReplicas != 2 || stored.Annotations["example.com/member-metadata"] != "retained" {
				t.Fatalf("unexpected member projection: %+v", stored)
			}
		})
	}
}

func TestPlannedDeleteRequiresDrainAndConditionalIdentity(t *testing.T) {
	for _, tt := range []struct {
		name       string
		requested  bool
		drained    bool
		current    int32
		race       bool
		absent     bool
		detached   bool
		editMember func(*v1beta1.InferenceService)
		editSource func(*v1beta1.InferenceService)
		wantErr    bool
	}{
		{name: "zero alone cannot delete", wantErr: true},
		{name: "requested drain awaits publication", requested: true, wantErr: true},
		{name: "positive allocation retained", current: 1, requested: true, drained: true, wantErr: true},
		{name: "detached zero cannot delete a positive allocation", current: 1, requested: true, drained: true, detached: true, wantErr: true},
		{name: "drained zero can delete", requested: true, drained: true},
		{name: "already absent member is harmless", requested: true, drained: true, absent: true},
		{name: "local service cannot be deleted", requested: true, drained: true, editMember: func(s *v1beta1.InferenceService) { s.Annotations, s.Labels = nil, nil }, wantErr: true},
		{name: "unidentified service cannot be deleted", requested: true, drained: true, editMember: func(s *v1beta1.InferenceService) { s.UID = "" }, wantErr: true},
		{name: "malformed policy cannot authorize deletion", requested: true, drained: true, editMember: func(s *v1beta1.InferenceService) { s.Annotations[constants.PlacementExecution] = "invalid" }, wantErr: true},
		{name: "newer member policy cannot authorize deletion", requested: true, drained: true, editMember: func(s *v1beta1.InferenceService) {
			raw, _ := protocol.Encode(&v1beta1.PlacementExecutionPolicy{PlanID: "plan-b", Revision: 3, SourceUID: "uid-1", ClusterUID: "member-a-uid", PauseSurge: true})
			s.Annotations[constants.PlacementExecution] = raw
		}, wantErr: true},
		{name: "newer source plan blocks deletion", requested: true, drained: true, editSource: func(s *v1beta1.InferenceService) { s.Status.Placement.Plan.Revision++ }, wantErr: true},
		{name: "racing member edit cannot be deleted", requested: true, drained: true, race: true, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			source := plannedTestSource()
			candidate := &source.Status.Placement.Candidates[0]
			candidate.Allocation.CurrentReplicas = tt.current
			candidate.Allocation.DrainRequested = tt.requested
			member := DeriveISVC(source, "", "")
			member.UID = "member-service-a"
			if tt.editMember != nil {
				tt.editMember(member)
			}
			withoutUID := member.UID == ""
			worker := emptyWorker(testScheme(t))
			if err := worker.Create(ctx, member); err != nil {
				t.Fatal(err)
			}
			if withoutUID {
				worker = interceptor.NewClient(worker, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if err := c.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					if _, ok := obj.(*v1beta1.InferenceService); ok {
						obj.SetUID("")
					}
					return nil
				}})
			}
			if tt.absent {
				if err := worker.Delete(ctx, member); err != nil {
					t.Fatal(err)
				}
			}
			base := worker
			worker = interceptor.NewClient(base, interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if tt.race {
					live := &v1beta1.InferenceService{}
					if err := c.Get(ctx, client.ObjectKeyFromObject(obj), live); err != nil {
						return err
					}
					live.Annotations["example.com/member-edit"] = "changed"
					if err := c.Update(ctx, live); err != nil {
						return err
					}
				}
				return c.Delete(ctx, obj, opts...)
			}})
			clusters := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(worker)}}, uid: "member-a-uid"}
			liveSource := source.DeepCopy()
			if tt.editSource != nil {
				tt.editSource(liveSource)
			}
			r, _ := newPlacer(testScheme(t), clusters, liveSource, plannedTestRegistration())
			requested := candidate.DeepCopy()
			if tt.detached {
				requested.Allocation.CurrentReplicas = 0
			}
			err := r.deletePlannedOn(ctx, source, *requested, tt.drained)
			if (err != nil) != tt.wantErr {
				t.Fatalf("delete error = %v, want error %t", err, tt.wantErr)
			}
			stored := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{}}
			getErr := worker.Get(ctx, client.ObjectKeyFromObject(member), stored)
			if tt.wantErr && getErr != nil {
				t.Fatalf("held member disappeared: %v", getErr)
			}
			if !tt.wantErr && client.IgnoreNotFound(getErr) != nil {
				t.Fatal(getErr)
			}
			if !tt.wantErr && getErr == nil {
				t.Fatal("drained member remains")
			}
		})
	}
}

func plannedTestRegistration() *v1beta1.WorkloadCluster {
	registration := readyWC("member-a", nil)
	registration.UID = "member-a-uid"
	return registration
}

func TestPlannedPolicyPreservesMemberSpec(t *testing.T) {
	for _, tt := range []struct {
		name                             string
		editSource                       func(*v1beta1.InferenceService)
		editMember                       func(*v1beta1.InferenceService)
		stale, absent, detached, wantErr bool
	}{
		{name: "pause changes only authority"},
		{name: "absent member is not created", absent: true},
		{name: "release retains member spec", editSource: func(s *v1beta1.InferenceService) { s.Status.Placement.Plan.PauseSurge = false }},
		{name: "source changed before pause", stale: true, wantErr: true},
		{name: "detached assignment cannot pause", detached: true, wantErr: true},
		{name: "local member is untouched", editMember: func(s *v1beta1.InferenceService) { s.Labels, s.Annotations = nil, nil }, wantErr: true},
		{name: "malformed policy is retained", editMember: func(s *v1beta1.InferenceService) { s.Annotations[constants.PlacementExecution] = "invalid" }, wantErr: true},
		{name: "newer member policy is retained", editMember: func(s *v1beta1.InferenceService) {
			raw, _ := protocol.Encode(&v1beta1.PlacementExecutionPolicy{PlanID: "newer", Revision: 3, SourceUID: "uid-1", ClusterUID: "member-a-uid", PauseSurge: true})
			s.Annotations[constants.PlacementExecution] = raw
		}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := plannedTestSource()
			source.Status.Placement.Plan.PauseSurge = true
			if tt.editSource != nil {
				tt.editSource(source)
			}
			member := DeriveISVC(source, "", "")
			member.UID = "member-uid"
			member.Spec.Engine.MinReplicas = ptr.To(7)
			member.Spec.Engine.MaxReplicas = 9
			member.Annotations["example.com/member-metadata"] = "retained"
			if tt.editMember != nil {
				tt.editMember(member)
			}
			worker := emptyWorker(testScheme(t))
			if !tt.absent {
				if err := worker.Create(t.Context(), member); err != nil {
					t.Fatal(err)
				}
			}
			before := member.DeepCopy()
			live := source.DeepCopy()
			if tt.stale {
				live.Generation++
			}
			connections := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(worker)}}, uid: "member-a-uid"}
			r, _ := newPlacer(testScheme(t), connections, live, plannedTestRegistration())
			candidate := *source.Status.Placement.Candidates[0].DeepCopy()
			if tt.detached {
				candidate.Allocation.CurrentReplicas++
			}
			err := r.syncPlannedPolicy(t.Context(), source, candidate)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("%s: %v", diff, err)
			}
			got := &v1beta1.InferenceService{}
			err = worker.Get(t.Context(), client.ObjectKeyFromObject(member), got)
			if tt.absent {
				if !apierrors.IsNotFound(err) {
					t.Fatalf("absence changed: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tt.wantErr {
				if diff := cmp.Diff(before, got); diff != "" {
					t.Error(diff)
				}
				return
			}
			policy, err := protocol.FromDerived(got)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(executionPolicy(source, candidate.Allocation), policy); diff != "" {
				t.Error(diff)
			}
			got.ResourceVersion = before.ResourceVersion
			delete(got.Annotations, constants.PlacementExecution)
			if diff := cmp.Diff(before, got); diff != "" {
				t.Errorf("pause changed member content (-want +got):\n%s", diff)
			}
		})
	}
}
