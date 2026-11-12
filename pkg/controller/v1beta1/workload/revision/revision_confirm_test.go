package revision

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// cachedView is the manager's client as the revision bookkeeping sees it:
// reads come from the copies the cache shows, writes go to the live store.
type cachedView struct {
	client.Client
	shows client.Reader
}

func (c *cachedView) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.shows.Get(ctx, key, obj, opts...)
}

func (c *cachedView) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return c.shows.List(ctx, list, opts...)
}

// twoReaderConfirm is a spec revision minted on the live store, with the
// inputs of its confirmation and the live reads and writes counted.
type twoReaderConfirm struct {
	replica   *v1beta1.InferenceReplica
	scope     *metav1.OwnerReference
	key       Key
	hash      string
	raw       []byte
	live      client.WithWatch
	liveReads *fakeclient.Calls
	writes    *fakeclient.Calls
}

func newTwoReaderConfirm(t *testing.T) *twoReaderConfirm {
	t.Helper()
	service := minimalISVC("svc", "prod", 1)
	replica := projectedReplica("svc-engine", "replica-1", service)
	scope := ScopeOwner(replica, testISVCGVK.GroupKind())
	hash, raw, err := Hash(basicPodSpecForRevision(), nil, nil, service.UID)
	if err != nil {
		t.Fatalf("Hash: %v", err)
	}
	liveReads := fakeclient.CountingVerb(fakeclient.Get).OnKind(&appsv1.ControllerRevision{})
	writes := fakeclient.CountingWrites()
	live := fakeclient.New(t, fakeclient.WithObjects(service, replica), liveReads, writes)
	s := &twoReaderConfirm{replica: replica, scope: scope, key: testKey(service, workload.ComponentEngine), hash: hash, raw: raw, live: live, liveReads: liveReads, writes: writes}
	if _, collision, err := EnsureControllerRevisionFromHash(context.Background(), live, live, replica, testIRGVK, s.key, hash, raw, scope); err != nil || collision {
		t.Fatalf("mint: collision=%v err=%v", collision, err)
	}
	return s
}

// stored reads the revision the live store holds.
func (s *twoReaderConfirm) stored(t *testing.T) *appsv1.ControllerRevision {
	t.Helper()
	cr := &appsv1.ControllerRevision{}
	if err := s.live.Get(context.Background(), client.ObjectKey{Namespace: s.key.Namespace, Name: Name(s.key, s.hash)}, cr); err != nil {
		t.Fatalf("read the stored revision: %v", err)
	}
	return cr
}

// confirmThrough confirms the revision with a cache that shows exactly the
// objects handed in and the live store as the live reader, and returns the
// answer with the live reads and the writes it made.
func (s *twoReaderConfirm) confirmThrough(t *testing.T, cached ...client.Object) (cr *appsv1.ControllerRevision, collision bool, liveReads, writes int) {
	t.Helper()
	view := &cachedView{Client: s.live, shows: fakeclient.New(t, fakeclient.WithObjects(cached...))}
	readsBefore, writesBefore := s.liveReads.Count(), s.writes.Count()
	cr, collision, err := EnsureControllerRevisionFromHash(context.Background(), view, s.live, s.replica, testIRGVK, s.key, s.hash, s.raw, s.scope)
	if err != nil {
		t.Fatalf("EnsureControllerRevisionFromHash: %v", err)
	}
	return cr, collision, s.liveReads.Count() - readsBefore, s.writes.Count() - writesBefore
}

// A revision the cache shows as this owner's, with the same payload and the
// scope owner on it, is confirmed from the cache: no live read, no write.
func TestEnsureControllerRevision_RevisionAtRestIsConfirmedFromTheCache(t *testing.T) {
	s := newTwoReaderConfirm(t)
	cr, collision, liveReads, writes := s.confirmThrough(t, s.stored(t))
	if collision || cr == nil || cr.Name != Name(s.key, s.hash) {
		t.Fatalf("confirm: collision=%v cr=%v", collision, cr)
	}
	if liveReads != 0 || writes != 0 {
		t.Fatalf("a revision at rest on the cached copy: live reads=%d writes=%d, want none", liveReads, writes)
	}
}

// A cached copy that does not name the scope owner yet is decided on the
// live object: the owner is written there, from a live base.
func TestEnsureControllerRevision_CopyWithoutTheScopeOwnerIsDecidedLive(t *testing.T) {
	s := newTwoReaderConfirm(t)
	behind := s.stored(t)
	behind.OwnerReferences = behind.OwnerReferences[:1]
	cr, collision, liveReads, writes := s.confirmThrough(t, behind)
	if collision || cr == nil {
		t.Fatalf("confirm: collision=%v cr=%v", collision, cr)
	}
	if liveReads != 1 {
		t.Fatalf("live reads=%d, want 1: a copy with something to write is decided on the live object", liveReads)
	}
	if writes != 0 {
		t.Fatalf("writes=%d, want 0: the live object already names the scope owner", writes)
	}
}

// A cache that does not show the revision yet is decided on the live
// object, which holds it: one live read, nothing minted.
func TestEnsureControllerRevision_MissingCopyIsDecidedLive(t *testing.T) {
	s := newTwoReaderConfirm(t)
	cr, collision, liveReads, writes := s.confirmThrough(t)
	if collision || cr == nil || cr.Name != Name(s.key, s.hash) {
		t.Fatalf("confirm: collision=%v cr=%v", collision, cr)
	}
	if liveReads != 1 || writes != 0 {
		t.Fatalf("live reads=%d writes=%d, want 1 and 0", liveReads, writes)
	}
}

// A cached copy another owner controls is no answer either way: the live
// object decides, and being this owner's it is confirmed with no collision.
func TestEnsureControllerRevision_CopyOfAnotherOwnerIsDecidedLive(t *testing.T) {
	s := newTwoReaderConfirm(t)
	other := minimalISVC("other", "prod", 1)
	otherOwned := s.stored(t)
	otherOwned.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(projectedReplica("other-engine", "replica-9", other), testIRGVK)}
	cr, collision, liveReads, writes := s.confirmThrough(t, otherOwned)
	if collision || cr == nil || !RetainedBy(cr, s.replica.UID, s.scope) {
		t.Fatalf("confirm: collision=%v retained=%v", collision, cr != nil && RetainedBy(cr, s.replica.UID, s.scope))
	}
	if liveReads != 1 || writes != 0 {
		t.Fatalf("live reads=%d writes=%d, want 1 and 0", liveReads, writes)
	}
}

// A cached copy whose payload differs from the templates is decided on the
// live object: a collision is reported from what the apiserver holds.
func TestEnsureControllerRevision_CopyWithAnotherPayloadIsDecidedLive(t *testing.T) {
	s := newTwoReaderConfirm(t)
	stale := s.stored(t)
	stale.Data.Raw = append([]byte(nil), `{"other":true}`...)
	live := s.stored(t)
	live.Data.Raw = stale.Data.Raw
	if err := s.live.Update(context.Background(), live); err != nil {
		t.Fatalf("rewrite the live payload: %v", err)
	}
	_, collision, liveReads, _ := s.confirmThrough(t, s.stored(t))
	if !collision {
		t.Fatal("a live revision with another payload is a collision")
	}
	if liveReads != 1 {
		t.Fatalf("live reads=%d, want 1", liveReads)
	}
}
