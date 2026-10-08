package replay

import (
	"context"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func newTestCluster(t *testing.T) *cluster {
	t.Helper()
	c, err := newCluster(clocktesting.NewFakeClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// A created replica gets a UID and a generation, and a spec write bumps the
// generation while a status write does not, as the apiserver does.
func TestClusterStampsIdentityAndGeneration(t *testing.T) {
	ctx := context.Background()
	c := newTestCluster(t)
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: "svc-a-engine", Namespace: "ns-a"}, Spec: v1beta1.InferenceReplicaSpec{Component: v1beta1.EngineComponent}}
	if err := c.cli.Create(ctx, ir); err != nil {
		t.Fatal(err)
	}
	if ir.UID != "svc-a-engine-uid" || ir.Generation != 1 {
		t.Fatalf("created replica uid=%q generation=%d", ir.UID, ir.Generation)
	}
	two := int32(2)
	ir.Spec.Replicas = &two
	if err := c.cli.Update(ctx, ir); err != nil {
		t.Fatal(err)
	}
	ir.Status.Replicas = 2
	if err := c.cli.Status().Update(ctx, ir); err != nil {
		t.Fatal(err)
	}
	stored := &v1beta1.InferenceReplica{}
	if err := c.cli.Get(ctx, client.ObjectKeyFromObject(ir), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Generation != 2 || stored.Status.Replicas != 2 {
		t.Fatalf("stored generation=%d status.replicas=%d", stored.Generation, stored.Status.Replicas)
	}
	patched := stored.DeepCopy()
	three := int32(3)
	patched.Spec.Replicas = &three
	if err := c.cli.Patch(ctx, patched, client.MergeFrom(stored)); err != nil {
		t.Fatal(err)
	}
	if err := c.cli.Get(ctx, client.ObjectKeyFromObject(ir), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Generation != 3 {
		t.Fatalf("generation after spec patch = %d, want 3", stored.Generation)
	}
}

// The cached client serves the frozen snapshot during a stale pass while
// writes still land on the store.
func TestClusterLaggedViewReadsTheSnapshot(t *testing.T) {
	ctx := context.Background()
	c := newTestCluster(t)
	c.snapshots = true
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc-a", Namespace: "ns-a", UID: "isvc-uid"}}
	if err := c.cli.Create(ctx, isvc); err != nil {
		t.Fatal(err)
	}
	if err := c.prepareSnapshot(ctx, 1); err != nil {
		t.Fatal(err)
	}
	c.finishPass()
	isvc.Annotations = map[string]string{"ome.io/rollout-repin": "now"}
	if err := c.cli.Update(ctx, isvc); err != nil {
		t.Fatal(err)
	}
	c.stalePass = true
	cached := &v1beta1.InferenceService{}
	if err := c.cachedClient().Get(ctx, client.ObjectKeyFromObject(isvc), cached); err != nil {
		t.Fatal(err)
	}
	if len(cached.Annotations) != 0 {
		t.Fatalf("stale read served the live annotations: %v", cached.Annotations)
	}
	// A write based on the stale copy is refused as the apiserver refuses
	// it; the flush re-reads live for its write base.
	cached.Status.Rollout = &v1beta1.RolloutStatus{}
	if err := c.cachedClient().Status().Update(ctx, cached); !apierrors.IsConflict(err) {
		t.Fatalf("write on the stale resourceVersion = %v, want a conflict", err)
	}
	live := &v1beta1.InferenceService{}
	if err := c.cli.Get(ctx, client.ObjectKeyFromObject(isvc), live); err != nil {
		t.Fatal(err)
	}
	live.Status.Rollout = &v1beta1.RolloutStatus{}
	if err := c.cachedClient().Status().Update(ctx, live); err != nil {
		t.Fatal(err)
	}
	if err := c.cli.Get(ctx, client.ObjectKeyFromObject(isvc), live); err != nil {
		t.Fatal(err)
	}
	if live.Status.Rollout == nil || live.Annotations["ome.io/rollout-repin"] != "now" {
		t.Fatalf("write through the lagged client did not land live: %+v", live)
	}
}

// An armed conflict answers the named logical status write and its retry
// counts as the same write; an armed flush failure is a non-conflict error.
func TestClusterRefusesStatusWrites(t *testing.T) {
	ctx := context.Background()
	c := newTestCluster(t)
	var records []writeRecord
	c.observe = func(w writeRecord) { records = append(records, w) }
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc-a", Namespace: "ns-a", UID: "isvc-uid"}}
	if err := c.cli.Create(ctx, isvc); err != nil {
		t.Fatal(err)
	}
	c.beginPass()
	c.armConflict(2, 1)
	write := func() error {
		live := &v1beta1.InferenceService{}
		if err := c.cli.Get(ctx, client.ObjectKeyFromObject(isvc), live); err != nil {
			return err
		}
		live.Status.Rollout = &v1beta1.RolloutStatus{}
		return c.cli.Status().Update(ctx, live)
	}
	if err := write(); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := write(); !apierrors.IsConflict(err) {
		t.Fatalf("second write = %v, want a conflict", err)
	}
	if err := write(); err != nil {
		t.Fatalf("retry of the second write: %v", err)
	}
	if c.statusWrites != 2 {
		t.Fatalf("logical writes = %d, want 2", c.statusWrites)
	}
	if unfired := c.unfiredConflicts(); len(unfired) != 0 {
		t.Fatalf("unfired = %v", unfired)
	}
	c.flushFailures = 1
	if err := write(); err == nil || apierrors.IsConflict(err) {
		t.Fatalf("armed failure = %v, want a non-conflict error", err)
	}
	if err := write(); err != nil {
		t.Fatalf("write after the failure: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("no writes recorded")
	}
}
