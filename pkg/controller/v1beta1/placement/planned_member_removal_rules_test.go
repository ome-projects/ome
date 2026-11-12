package placement

import (
	"context"
	"fmt"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/protocol"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// drainedSource is a source whose plan drained member-a to zero.
func drainedSource() *v1beta1.InferenceService {
	source := plannedTestSource()
	a := source.Status.Placement.Candidates[0].Allocation
	a.CurrentReplicas, a.DesiredReplicas, a.DrainRequested = 0, 0, true
	return source
}

// removalMember is member-a's copy of the source, as placement created it.
func removalMember(source *v1beta1.InferenceService) *v1beta1.InferenceService {
	member := DeriveISVC(source, "", "")
	member.UID = "member-service-a"
	return member
}

// memberPolicy encodes a policy of this source's plan, edited by the caller.
func memberPolicy(t *testing.T, edit func(*v1beta1.PlacementExecutionPolicy)) string {
	t.Helper()
	policy := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 2, SourceUID: "uid-1", ClusterUID: "member-a-uid"}
	if edit != nil {
		edit(policy)
	}
	raw, err := protocol.Encode(policy)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// removalPlacer is a placer whose member-a transport is the worker, bound to
// the registration UID the transport answers for.
func removalPlacer(t *testing.T, worker client.WithWatch, source *v1beta1.InferenceService, uid types.UID) *Reconciler {
	t.Helper()
	clusters := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(worker)}}, uid: uid}
	r, _ := newPlacer(testScheme(t), clusters, source.DeepCopy(), plannedTestRegistration())
	return r
}

// storedMember reads member-a's copy back, or nil when it is gone.
func storedMember(t *testing.T, worker client.Client) *v1beta1.InferenceService {
	t.Helper()
	member := &v1beta1.InferenceService{}
	err := worker.Get(context.Background(), client.ObjectKey{Namespace: "team-a", Name: "svc"}, member)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return member
}

// TestPlannedRemovalRefusalsNameTheRule holds the words of the two refusals
// the removal gives on its own: an allocation that is not drained to zero,
// and a member whose identity cannot be verified.
func TestPlannedRemovalRefusalsNameTheRule(t *testing.T) {
	const allocationText = "member removal requires a drained zero allocation"
	const identityText = "member identity cannot authorize removal"
	for _, tt := range []struct {
		name       string
		drained    bool
		editSource func(*v1beta1.InferenceService)
		editMember func(*v1beta1.InferenceService)
		readMember func(*v1beta1.InferenceService)
		want       string
	}{
		{name: "the drain was not verified", want: allocationText},
		{name: "the plan did not ask for a drain", drained: true, editSource: func(s *v1beta1.InferenceService) { s.Status.Placement.Candidates[0].Allocation.DrainRequested = false }, want: allocationText},
		{name: "the member still runs a replica", drained: true, editSource: func(s *v1beta1.InferenceService) { s.Status.Placement.Candidates[0].Allocation.CurrentReplicas = 1 }, want: allocationText},
		{name: "the member is not this source's copy", drained: true, editMember: func(m *v1beta1.InferenceService) { m.Labels, m.Annotations = nil, nil }, want: identityText},
		{name: "the member has no UID", drained: true, readMember: func(m *v1beta1.InferenceService) { m.SetUID("") }, want: identityText},
		{name: "the member has no resource version", drained: true, readMember: func(m *v1beta1.InferenceService) { m.SetResourceVersion("") }, want: identityText},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			source := drainedSource()
			if tt.editSource != nil {
				tt.editSource(source)
			}
			member := removalMember(source)
			if tt.editMember != nil {
				tt.editMember(member)
			}
			stored := emptyWorker(testScheme(t))
			if err := stored.Create(ctx, member); err != nil {
				t.Fatal(err)
			}
			worker := stored
			if tt.readMember != nil {
				worker = fakeclient.Wrap(t, stored, fakeclient.EditedRead(tt.readMember))
			}
			r := removalPlacer(t, worker, source, "member-a-uid")
			err := r.deletePlannedOn(ctx, source, source.Status.Placement.Candidates[0], tt.drained)
			if err == nil || err.Error() != tt.want {
				t.Fatalf("deletePlannedOn = %v, want %q", err, tt.want)
			}
			if storedMember(t, stored) == nil {
				t.Fatal("the refused member was deleted")
			}
		})
	}
}

// TestPlannedRemovalChecksTheAllocationBeforeTheTransport: an allocation that
// is not drained is refused before the member cluster is reached, so a
// cluster without a connection reports the allocation first.
func TestPlannedRemovalChecksTheAllocationBeforeTheTransport(t *testing.T) {
	ctx := context.Background()
	source := drainedSource()
	stored := emptyWorker(testScheme(t))
	if err := stored.Create(ctx, removalMember(source)); err != nil {
		t.Fatal(err)
	}
	r := removalPlacer(t, stored, source, "another-uid")
	candidate := source.Status.Placement.Candidates[0]
	if err := r.deletePlannedOn(ctx, source, candidate, false); err == nil || err.Error() != "member removal requires a drained zero allocation" {
		t.Fatalf("undrained removal = %v, want the allocation refusal", err)
	}
	if err := r.deletePlannedOn(ctx, source, candidate, true); err == nil || err.Error() != `cluster "member-a" has no connection for its accepted UID` {
		t.Fatalf("drained removal without a transport = %v, want the transport refusal", err)
	}
}

// TestPlannedRemovalAcceptsAPolicyThePlanMayFollow: a member whose policy is
// the plan's own, an older revision of it, or an older one that recorded
// replica minimums is deleted.
func TestPlannedRemovalAcceptsAPolicyThePlanMayFollow(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*v1beta1.PlacementExecutionPolicy)
	}{
		{name: "the plan's own policy"},
		{name: "an older revision", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.Revision = 1 }},
		{name: "an older revision with replica minimums", edit: func(p *v1beta1.PlacementExecutionPolicy) {
			p.Revision = 1
			p.ReplicaFloors = []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 2}}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			source := drainedSource()
			member := removalMember(source)
			member.Annotations[constants.PlacementExecution] = memberPolicy(t, tt.edit)
			stored := emptyWorker(testScheme(t))
			if err := stored.Create(ctx, member); err != nil {
				t.Fatal(err)
			}
			r := removalPlacer(t, stored, source, "member-a-uid")
			if err := r.deletePlannedOn(ctx, source, source.Status.Placement.Candidates[0], true); err != nil {
				t.Fatalf("deletePlannedOn = %v, want nil", err)
			}
			if storedMember(t, stored) != nil {
				t.Fatal("the drained member remains")
			}
		})
	}
}

// TestPlannedRemovalRefusesAPolicyThePlanMayNotFollow holds the protocol's
// words for a member policy of another source, of another cluster, or of the
// plan's revision with other content; the member stays.
func TestPlannedRemovalRefusesAPolicyThePlanMayNotFollow(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*v1beta1.PlacementExecutionPolicy)
		want string
	}{
		{name: "another source", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.SourceUID = "uid-2" }, want: "placement execution policy does not belong to the derived service's source"},
		{name: "another cluster", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.ClusterUID = "member-b-uid" }, want: "placement execution identity changed"},
		{name: "the same revision with other content", edit: func(p *v1beta1.PlacementExecutionPolicy) { p.PauseSurge = true }, want: "placement execution revision is stale or conflicting"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			source := drainedSource()
			member := removalMember(source)
			member.Annotations[constants.PlacementExecution] = memberPolicy(t, tt.edit)
			stored := emptyWorker(testScheme(t))
			if err := stored.Create(ctx, member); err != nil {
				t.Fatal(err)
			}
			r := removalPlacer(t, stored, source, "member-a-uid")
			err := r.deletePlannedOn(ctx, source, source.Status.Placement.Candidates[0], true)
			if err == nil || err.Error() != tt.want {
				t.Fatalf("deletePlannedOn = %v, want %q", err, tt.want)
			}
			if storedMember(t, stored) == nil {
				t.Fatal("the refused member was deleted")
			}
		})
	}
}

// deleteInterceptor wraps the stored worker with a Delete hook.
func deleteInterceptor(t *testing.T, stored client.WithWatch, hook func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error) client.WithWatch {
	t.Helper()
	return fakeclient.Wrap(t, stored, fakeclient.WithInterceptors(interceptor.Funcs{Delete: hook}))
}

// TestPlannedRemovalDeletesUnderTheMembersIdentityAndVersion: the delete
// names the UID and resource version the removal read, so it removes that
// object and no other.
func TestPlannedRemovalDeletesUnderTheMembersIdentityAndVersion(t *testing.T) {
	ctx := context.Background()
	source := drainedSource()
	stored := emptyWorker(testScheme(t))
	if err := stored.Create(ctx, removalMember(source)); err != nil {
		t.Fatal(err)
	}
	before := storedMember(t, stored)
	var seen *client.DeleteOptions
	worker := deleteInterceptor(t, stored, func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		seen = &client.DeleteOptions{}
		seen.ApplyOptions(opts)
		return c.Delete(ctx, obj, opts...)
	})
	r := removalPlacer(t, worker, source, "member-a-uid")
	if err := r.deletePlannedOn(ctx, source, source.Status.Placement.Candidates[0], true); err != nil {
		t.Fatal(err)
	}
	if seen == nil || seen.Preconditions == nil || seen.Preconditions.UID == nil || seen.Preconditions.ResourceVersion == nil {
		t.Fatalf("delete options = %+v, want a UID and resource version precondition", seen)
	}
	if *seen.Preconditions.UID != before.UID || *seen.Preconditions.ResourceVersion != before.ResourceVersion {
		t.Fatalf("delete preconditions = %s/%s, want %s/%s", *seen.Preconditions.UID, *seen.Preconditions.ResourceVersion, before.UID, before.ResourceVersion)
	}
	if storedMember(t, stored) != nil {
		t.Fatal("the drained member remains")
	}
}

// TestPlannedRemovalLeavesAMemberReplacedUnderItsName: a copy recreated
// under the same name between the read and the delete is another object; the
// delete is refused and the new copy stays.
func TestPlannedRemovalLeavesAMemberReplacedUnderItsName(t *testing.T) {
	ctx := context.Background()
	source := drainedSource()
	stored := emptyWorker(testScheme(t))
	if err := stored.Create(ctx, removalMember(source)); err != nil {
		t.Fatal(err)
	}
	worker := deleteInterceptor(t, stored, func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		live := &v1beta1.InferenceService{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), live); err != nil {
			return err
		}
		if err := c.Delete(ctx, live); err != nil {
			return err
		}
		replacement := live.DeepCopy()
		replacement.ResourceVersion, replacement.UID = "", "member-service-b"
		if err := c.Create(ctx, replacement); err != nil {
			return err
		}
		replacement.Annotations["example.com/recreated"] = "yes"
		if err := c.Update(ctx, replacement); err != nil {
			return err
		}
		return c.Delete(ctx, obj, opts...)
	})
	r := removalPlacer(t, worker, source, "member-a-uid")
	err := r.deletePlannedOn(ctx, source, source.Status.Placement.Candidates[0], true)
	if !apierrors.IsConflict(err) {
		t.Fatalf("deletePlannedOn = %v, want a conflict", err)
	}
	if after := storedMember(t, stored); after == nil || after.UID != "member-service-b" {
		t.Fatalf("the replacement is gone or changed: %+v", after)
	}
}

// TestPlannedRemovalTreatsAMemberGoneAtTheDeleteAsRemoved: a member that
// disappears between the read and the delete is removed, not an error.
func TestPlannedRemovalTreatsAMemberGoneAtTheDeleteAsRemoved(t *testing.T) {
	ctx := context.Background()
	source := drainedSource()
	stored := emptyWorker(testScheme(t))
	if err := stored.Create(ctx, removalMember(source)); err != nil {
		t.Fatal(err)
	}
	worker := deleteInterceptor(t, stored, func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		return apierrors.NewNotFound(schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}, obj.GetName())
	})
	r := removalPlacer(t, worker, source, "member-a-uid")
	if err := r.deletePlannedOn(ctx, source, source.Status.Placement.Candidates[0], true); err != nil {
		t.Fatalf("deletePlannedOn = %v, want nil", err)
	}
}

// TestPlannedRemovalReturnsTheClustersRefusals: a read failure that is not
// absence, and a delete the member cluster refuses, come back as they are and
// the member stays.
func TestPlannedRemovalReturnsTheClustersRefusals(t *testing.T) {
	for _, tt := range []struct {
		name  string
		funcs interceptor.Funcs
		want  string
	}{
		{name: "a read failure", funcs: interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			return apierrors.NewInternalError(fmt.Errorf("member read failed"))
		}}, want: "Internal error occurred: member read failed"},
		{name: "a refused delete", funcs: interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			return apierrors.NewForbidden(schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}, obj.GetName(), fmt.Errorf("denied"))
		}}, want: `inferenceservices.ome.io "svc" is forbidden: denied`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			source := drainedSource()
			stored := emptyWorker(testScheme(t))
			if err := stored.Create(ctx, removalMember(source)); err != nil {
				t.Fatal(err)
			}
			worker := fakeclient.Wrap(t, stored, fakeclient.WithInterceptors(tt.funcs))
			r := removalPlacer(t, worker, source, "member-a-uid")
			err := r.deletePlannedOn(ctx, source, source.Status.Placement.Candidates[0], true)
			if err == nil || err.Error() != tt.want {
				t.Fatalf("deletePlannedOn = %v, want %q", err, tt.want)
			}
			if storedMember(t, stored) == nil {
				t.Fatal("the member was deleted")
			}
		})
	}
}

// TestPlannedRemovalChecksThePlanBeforeTheMember: a plan that moved on is
// refused before the member cluster is read, so a cluster that cannot be
// read reports the plan first.
func TestPlannedRemovalChecksThePlanBeforeTheMember(t *testing.T) {
	ctx := context.Background()
	source := drainedSource()
	stored := emptyWorker(testScheme(t))
	if err := stored.Create(ctx, removalMember(source)); err != nil {
		t.Fatal(err)
	}
	worker := fakeclient.Wrap(t, stored, fakeclient.WithInterceptors(interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		return apierrors.NewInternalError(fmt.Errorf("member read failed"))
	}}))
	moved := source.DeepCopy()
	moved.Status.Placement.Plan.Revision++
	clusters := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": workloadcluster.NewNeverCachingClient(worker)}}, uid: "member-a-uid"}
	r, _ := newPlacer(testScheme(t), clusters, moved, plannedTestRegistration())
	err := r.deletePlannedOn(ctx, source, source.Status.Placement.Candidates[0], true)
	if err == nil || err.Error() != "placement source or plan changed" {
		t.Fatalf("deletePlannedOn = %v, want the plan refusal", err)
	}
}
