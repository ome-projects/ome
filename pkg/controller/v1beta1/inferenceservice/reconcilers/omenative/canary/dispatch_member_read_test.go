package canary

import (
	"context"
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

// A pass reads each member's replica through the live reader once: the
// observation of its rows keeps the replica as read, the Services sweep
// works from that object, and the rollback signal writes it instead of
// reading the member again.

// TestDispatchReadsEachMembersReplicaLiveOnce holds the live reads of a
// pass at rest to one replica read and one pod list per member, settled
// and holding.
func TestDispatchReadsEachMembersReplicaLiveOnce(t *testing.T) {
	shape := dispatchFleetShapes()[3]
	for _, state := range []dispatchFleetState{dispatchSettled, dispatchHolding} {
		f := buildDispatchFleet(t, shape, state)
		f.pass(t)
		if members := int64(len(shape.members)); f.live.gets != members || f.live.lists != members {
			t.Fatalf("%s/%s: a pass read %d replicas and %d pod lists live, want %d of each", shape.name, state, f.live.gets, f.live.lists, members)
		}
	}
}

// memberReadFixture is one member whose replica stores its rows in the
// column form, in a fake client that stores what a write carries, with the
// ControllerRevision of its stable revision.
func memberReadFixture(t *testing.T) (*v1beta1.InferenceService, *v1beta1.InferenceReplica, client.Client) {
	t.Helper()
	shape := dispatchFleetShape{name: "engine/64x1", members: []dispatchFleetMember{{v1beta1.EngineComponent, 64, 0}}}
	isvc := dispatchFleetService("serving", shape, dispatchHolding)
	replica := dispatchFleetReplica(t, isvc, shape.members[0], dispatchHolding)
	if replica.Status.InstanceStatusColumns == nil {
		t.Fatal("the fixture's rows should be stored in the column form")
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithObjects(
		replica,
		dispatchFleetRevision(isvc, v1beta1.EngineComponent, dispatchStableHash),
		dispatchFleetRevision(isvc, v1beta1.EngineComponent, dispatchTargetHash),
	).Build()
	return isvc, replica, c
}

// replicaReadCounter counts the InferenceReplica reads made through it.
type replicaReadCounter struct {
	client.Reader
	replicaGets int
}

func (r *replicaReadCounter) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1beta1.InferenceReplica); ok {
		r.replicaGets++
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

// TestObserveCanaryRevisionsKeepsTheReplicaAsRead holds the observation to
// decoded rows beside the replica in its stored form.
func TestObserveCanaryRevisionsKeepsTheReplicaAsRead(t *testing.T) {
	isvc, replica, c := memberReadFixture(t)
	reads := irstatus.NewReader(c, irstatus.NewDecoder(dispatchDecodedRows))
	observed, err := observeCanaryRevisions(context.Background(), reads, isvc, v1beta1.EngineComponent)
	if err != nil {
		t.Fatal(err)
	}
	if !observed.fromIR || len(observed.rows) != 64 || observed.targetHash != dispatchTargetHash {
		t.Fatalf("the observation should carry the 64 decoded rows and the target, got %d rows, target %q, fromIR %v", len(observed.rows), observed.targetHash, observed.fromIR)
	}
	if observed.stored == nil || observed.stored.Status.InstanceStatusColumns == nil || len(observed.stored.Status.InstanceStatuses) != 0 {
		t.Fatalf("the observation should keep the replica as read, in its column form, got %+v", observed.stored)
	}
	if observed.stored.UID != replica.UID || observed.stored.ResourceVersion == "" {
		t.Fatalf("the kept replica should be the one read, with its resource version, got %+v", observed.stored.ObjectMeta)
	}
}

// TestRollbackSignalWritesTheReplicaThePassRead holds the rollback signal
// to the replica the pass observed: no second read of it, and the target
// written on the object as it was read, its rows still in the column form.
func TestRollbackSignalWritesTheReplicaThePassRead(t *testing.T) {
	isvc, replica, c := memberReadFixture(t)
	counter := &replicaReadCounter{Reader: c}
	reads := irstatus.NewReader(counter, irstatus.NewDecoder(dispatchDecodedRows))
	ctx := context.Background()
	observed, err := observeCanaryRevisions(ctx, reads, isvc, v1beta1.EngineComponent)
	if err != nil {
		t.Fatal(err)
	}
	counter.replicaGets = 0
	if _, err := reconcileRollbackSignal(ctx, c, reads, isvc, v1beta1.EngineComponent, observed.stored, dispatchStableHash, true, false); err != nil {
		t.Fatal(err)
	}
	if counter.replicaGets != 0 {
		t.Fatalf("the rollback signal read the replica %d times although the pass had observed it", counter.replicaGets)
	}
	written := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(replica), written); err != nil {
		t.Fatal(err)
	}
	want := dispatchRevisionName(isvc, v1beta1.EngineComponent, dispatchStableHash)
	if written.Spec.Pacing == nil || written.Spec.Pacing.RollbackToRevision == nil || *written.Spec.Pacing.RollbackToRevision != want {
		t.Fatalf("the rollback target should name %s, got %+v", want, written.Spec.Pacing)
	}
	if written.Status.InstanceStatusColumns == nil || len(written.Status.InstanceStatuses) != 0 {
		t.Fatalf("the write should carry the replica as read, its rows in the column form, got %d rows and columns %v", len(written.Status.InstanceStatuses), written.Status.InstanceStatusColumns != nil)
	}
}

// conflictingClient answers every update of a replica with a conflict
// while conflicts is above zero, as the apiserver does for an object that
// moved under the writer; later updates go through.
type conflictingClient struct {
	client.Client
	conflicts int
}

func (c *conflictingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if _, ok := obj.(*v1beta1.InferenceReplica); ok && c.conflicts > 0 {
		c.conflicts--
		return apierrors.NewConflict(schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "inferencereplicas"}, obj.GetName(), errors.New("the object has been modified"))
	}
	return c.Client.Update(ctx, obj, opts...)
}

// TestRollbackSignalDecidesAgainFromAFreshReadOnAConflict holds the
// rollback signal, when the replica the pass read moved under it, to one
// fresh read of the member and the target written on that read; a conflict
// on the fresh read's write is handed back.
func TestRollbackSignalDecidesAgainFromAFreshReadOnAConflict(t *testing.T) {
	isvc, replica, c := memberReadFixture(t)
	counter := &replicaReadCounter{Reader: c}
	reads := irstatus.NewReader(counter, irstatus.NewDecoder(dispatchDecodedRows))
	ctx := context.Background()
	observed, err := observeCanaryRevisions(ctx, reads, isvc, v1beta1.EngineComponent)
	if err != nil {
		t.Fatal(err)
	}
	// The replica controller writes the replica after the pass read it.
	moved := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(replica), moved); err != nil {
		t.Fatal(err)
	}
	moved.Labels = map[string]string{"example.com/moved": "after-the-read"}
	if err := c.Update(ctx, moved); err != nil {
		t.Fatal(err)
	}
	counter.replicaGets = 0
	if _, err := reconcileRollbackSignal(ctx, c, reads, isvc, v1beta1.EngineComponent, observed.stored, dispatchStableHash, true, false); err != nil {
		t.Fatalf("the signal should decide again from a fresh read, got %v", err)
	}
	if counter.replicaGets != 1 {
		t.Fatalf("the signal should read the member once after the conflict, read it %d times", counter.replicaGets)
	}
	written := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(replica), written); err != nil {
		t.Fatal(err)
	}
	want := dispatchRevisionName(isvc, v1beta1.EngineComponent, dispatchStableHash)
	if written.Spec.Pacing == nil || written.Spec.Pacing.RollbackToRevision == nil || *written.Spec.Pacing.RollbackToRevision != want || written.Labels["example.com/moved"] != "after-the-read" {
		t.Fatalf("the rollback target should name %s on the replica as it moved, got %+v with labels %v", want, written.Spec.Pacing, written.Labels)
	}

	// The fresh read's write conflicts too: no third attempt, the error
	// ends the pass.
	isvc, _, c = memberReadFixture(t)
	counter = &replicaReadCounter{Reader: c}
	reads = irstatus.NewReader(counter, irstatus.NewDecoder(dispatchDecodedRows))
	observed, err = observeCanaryRevisions(ctx, reads, isvc, v1beta1.EngineComponent)
	if err != nil {
		t.Fatal(err)
	}
	counter.replicaGets = 0
	writes := &conflictingClient{Client: c, conflicts: 2}
	_, err = reconcileRollbackSignal(ctx, writes, reads, isvc, v1beta1.EngineComponent, observed.stored, dispatchStableHash, true, false)
	if !apierrors.IsConflict(err) {
		t.Fatalf("a second conflict should be handed back, got %v", err)
	}
	if counter.replicaGets != 1 || writes.conflicts != 0 {
		t.Fatalf("the signal should read once and write twice, read %d times with %d conflicts left", counter.replicaGets, writes.conflicts)
	}
}

// TestRollbackSignalReadsTheReplicaWithoutAnObservation holds the rollback
// signal to one live read of a member the pass holds no observation of.
func TestRollbackSignalReadsTheReplicaWithoutAnObservation(t *testing.T) {
	isvc, replica, c := memberReadFixture(t)
	counter := &replicaReadCounter{Reader: c}
	reads := irstatus.NewReader(counter, irstatus.NewDecoder(dispatchDecodedRows))
	ctx := context.Background()
	if _, err := reconcileRollbackSignal(ctx, c, reads, isvc, v1beta1.EngineComponent, nil, dispatchStableHash, true, false); err != nil {
		t.Fatal(err)
	}
	if counter.replicaGets != 1 {
		t.Fatalf("the rollback signal should read an unobserved member once, read it %d times", counter.replicaGets)
	}
	written := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(replica), written); err != nil {
		t.Fatal(err)
	}
	want := dispatchRevisionName(isvc, v1beta1.EngineComponent, dispatchStableHash)
	if written.Spec.Pacing == nil || written.Spec.Pacing.RollbackToRevision == nil || *written.Spec.Pacing.RollbackToRevision != want {
		t.Fatalf("the rollback target should name %s, got %+v", want, written.Spec.Pacing)
	}
}
