package revision

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// twoReaderHistory is a revision history minted on the live store, the
// newest revision the one every row runs, with the live lists and the
// writes counted.
type twoReaderHistory struct {
	key       Key
	names     []string
	live      client.WithWatch
	liveLists *fakeclient.Calls
	writes    *fakeclient.Calls
}

func newTwoReaderHistory(t *testing.T, minted int) *twoReaderHistory {
	t.Helper()
	service := minimalISVC("svc", "prod", 1)
	replica := projectedReplica("svc-engine", "replica-1", service)
	scope := ScopeOwner(replica, testISVCGVK.GroupKind())
	liveLists := fakeclient.CountingVerb(fakeclient.List).OnKind(&appsv1.ControllerRevisionList{})
	writes := fakeclient.CountingWrites()
	live := fakeclient.New(t, fakeclient.WithObjects(service, replica), liveLists, writes)
	h := &twoReaderHistory{key: testKey(service, workload.ComponentEngine), live: live, liveLists: liveLists, writes: writes}
	for i := 0; i < minted; i++ {
		template := Template{PodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: fmt.Sprintf("serving:v%d", i+1)}}}}
		hash, raw, err := HashTemplate(template, nil, service.UID)
		if err != nil {
			t.Fatalf("HashTemplate: %v", err)
		}
		cr, collision, err := EnsureControllerRevisionFromHash(context.Background(), live, live, replica, testIRGVK, h.key, hash, raw, scope)
		if err != nil || collision {
			t.Fatalf("mint revision %d: collision=%v err=%v", i+1, collision, err)
		}
		h.names = append(h.names, cr.Name)
	}
	return h
}

// running is the revision every row runs: the newest.
func (h *twoReaderHistory) running() string { return h.names[len(h.names)-1] }

// stored reads the history the live store holds, oldest first.
func (h *twoReaderHistory) stored(t *testing.T) []client.Object {
	t.Helper()
	list := &appsv1.ControllerRevisionList{}
	if err := h.live.List(context.Background(), list); err != nil {
		t.Fatalf("list the stored history: %v", err)
	}
	slices.SortFunc(list.Items, func(a, b appsv1.ControllerRevision) int { return int(a.Revision - b.Revision) })
	out := make([]client.Object, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, &list.Items[i])
	}
	return out
}

// storedNames is the names of the stored history, oldest first.
func (h *twoReaderHistory) storedNames(t *testing.T) []string {
	t.Helper()
	var names []string
	for _, cr := range h.stored(t) {
		names = append(names, cr.GetName())
	}
	return names
}

// sweepThrough sweeps the history with a cache that shows exactly the
// objects handed in and the live store as the live reader, and returns the
// live lists and the writes the sweep made.
func (h *twoReaderHistory) sweepThrough(t *testing.T, limit int, cache client.Reader) (liveLists, writes int) {
	t.Helper()
	view := &cachedView{Client: h.live, shows: cache}
	listsBefore, writesBefore := h.liveLists.Count(), h.writes.Count()
	if err := RetainControllerRevisions(context.Background(), view, h.live, h.key, limit, h.running()); err != nil {
		t.Fatalf("RetainControllerRevisions: %v", err)
	}
	return h.liveLists.Count() - listsBefore, h.writes.Count() - writesBefore
}

// cacheShowing is a cache that shows exactly the objects handed in.
func cacheShowing(t *testing.T, objects ...client.Object) client.Reader {
	t.Helper()
	return fakeclient.New(t, fakeclient.WithObjects(objects...))
}

// A history the cache shows at the limit is decided on the cache: no live
// list, nothing deleted.
func TestRetainControllerRevisions_AHistoryAtTheLimitIsDecidedOnTheCache(t *testing.T) {
	h := newTwoReaderHistory(t, 11)
	liveLists, writes := h.sweepThrough(t, 10, cacheShowing(t, h.stored(t)...))
	if liveLists != 0 || writes != 0 {
		t.Fatalf("a history at the limit on the cached copy: live lists=%d writes=%d, want none", liveLists, writes)
	}
	if got := h.storedNames(t); !slices.Equal(got, h.names) {
		t.Fatalf("the history is %v, want every revision kept: %v", got, h.names)
	}
}

// A history the cache shows over the limit is decided on the live list: one
// live list, and the oldest revisions past the limit deleted from it.
func TestRetainControllerRevisions_AHistoryOverTheLimitIsDecidedOnTheLiveList(t *testing.T) {
	h := newTwoReaderHistory(t, 13)
	liveLists, writes := h.sweepThrough(t, 10, cacheShowing(t, h.stored(t)...))
	if liveLists != 1 {
		t.Fatalf("live lists=%d, want 1: a deletion is decided on the live list", liveLists)
	}
	if writes != 2 {
		t.Fatalf("writes=%d, want 2: the two oldest revisions past the limit", writes)
	}
	if got, want := h.storedNames(t), h.names[2:]; !slices.Equal(got, want) {
		t.Fatalf("the history is %v, want the two oldest gone: %v", got, want)
	}
}

// A cache that does not show a revision minted since keeps the history as
// it is, one revision past the limit, until it shows it; the sweep that
// sees the whole history lists live and deletes the oldest.
func TestRetainControllerRevisions_ACacheBehindAMintKeepsTheHistoryUntilItShowsIt(t *testing.T) {
	h := newTwoReaderHistory(t, 12)
	whole := h.stored(t)
	mintedSince := whole[len(whole)-2]
	behind := slices.DeleteFunc(slices.Clone(whole), func(o client.Object) bool { return o.GetName() == mintedSince.GetName() })

	liveLists, writes := h.sweepThrough(t, 10, cacheShowing(t, behind...))
	if liveLists != 0 || writes != 0 {
		t.Fatalf("a cache behind a mint: live lists=%d writes=%d, want none", liveLists, writes)
	}
	if got := h.storedNames(t); !slices.Equal(got, h.names) {
		t.Fatalf("the history is %v, want every revision kept: %v", got, h.names)
	}

	liveLists, writes = h.sweepThrough(t, 10, cacheShowing(t, whole...))
	if liveLists != 1 || writes != 1 {
		t.Fatalf("the cache showing the whole history: live lists=%d writes=%d, want 1 and 1", liveLists, writes)
	}
	if got, want := h.storedNames(t), h.names[1:]; !slices.Equal(got, want) {
		t.Fatalf("the history is %v, want the oldest gone: %v", got, want)
	}
}

// A cache that still shows a revision the live store no longer holds is
// decided on the live list: one live list, nothing deleted, no delete of
// the gone revision issued.
func TestRetainControllerRevisions_ACacheShowingADeletedRevisionIsDecidedLive(t *testing.T) {
	h := newTwoReaderHistory(t, 11)
	whole := h.stored(t)
	gone := whole[0].(*appsv1.ControllerRevision).DeepCopy()
	gone.Name, gone.Revision, gone.ResourceVersion, gone.UID = gone.Name+"-gone", 0, "", ""

	liveLists, writes := h.sweepThrough(t, 10, cacheShowing(t, append(whole, gone)...))
	if liveLists != 1 {
		t.Fatalf("live lists=%d, want 1: a cache over the limit sends the sweep to the live list", liveLists)
	}
	if writes != 0 {
		t.Fatalf("writes=%d, want 0: the live history is at the limit", writes)
	}
	if got := h.storedNames(t); !slices.Equal(got, h.names) {
		t.Fatalf("the history is %v, want every revision kept: %v", got, h.names)
	}
}

// A cache that cannot list the history is decided on the live list.
func TestRetainControllerRevisions_ACacheThatCannotListIsDecidedLive(t *testing.T) {
	h := newTwoReaderHistory(t, 13)
	refusing := fakeclient.New(t, fakeclient.FailingVerb(fakeclient.List, fakeclient.EveryCall, errors.New("cache not synced")).OnKind(&appsv1.ControllerRevisionList{}))
	liveLists, writes := h.sweepThrough(t, 10, refusing)
	if liveLists != 1 || writes != 2 {
		t.Fatalf("a cache that cannot list: live lists=%d writes=%d, want 1 and 2", liveLists, writes)
	}
	if got, want := h.storedNames(t), h.names[2:]; !slices.Equal(got, want) {
		t.Fatalf("the history is %v, want the two oldest gone: %v", got, want)
	}
}

// A revision a live name protects is kept whichever list decides: the
// oldest revision, named as the one a row still runs, survives a sweep over
// the limit beside the newest kept ones.
func TestRetainControllerRevisions_ALiveNameIsProtectedOnBothLists(t *testing.T) {
	h := newTwoReaderHistory(t, 13)
	oldest := h.names[0]
	view := &cachedView{Client: h.live, shows: cacheShowing(t, h.stored(t)...)}
	if err := RetainControllerRevisions(context.Background(), view, h.live, h.key, 10, h.running(), oldest); err != nil {
		t.Fatalf("RetainControllerRevisions: %v", err)
	}
	got := h.storedNames(t)
	want := append([]string{oldest}, h.names[2:]...)
	if !slices.Equal(got, want) {
		t.Fatalf("the history is %v, want the protected oldest and the ten newest beside the running one: %v", got, want)
	}
}
