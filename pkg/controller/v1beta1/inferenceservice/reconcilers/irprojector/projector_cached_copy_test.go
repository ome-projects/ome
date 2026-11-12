package irprojector

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// twoReaderProjection is a projection whose cache and live reader are two
// stores: the live store holds the replica the apiserver holds, the cache
// store the copy the informer shows this pass, with the live reads and the
// writes counted.
type twoReaderProjection struct {
	isvc      *v1beta1.InferenceService
	key       types.NamespacedName
	live      client.WithWatch
	liveReads *fakeclient.Calls
	writes    *fakeclient.Calls
}

// newTwoReaderProjection projects the service's engine once onto the live
// store, so the store holds the replica as the projector writes it.
func newTwoReaderProjection(t *testing.T) *twoReaderProjection {
	t.Helper()
	g := gomega.NewWithT(t)
	isvc := baselineISVC("example", "prod")
	liveReads := fakeclient.CountingVerb(fakeclient.Get).OnKind(&v1beta1.InferenceReplica{})
	writes := fakeclient.CountingWrites()
	live := fakeclient.New(t, liveReads, writes)
	_, err := EnsureInferenceReplica(context.Background(), minimalParams(t, isvc, live))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	return &twoReaderProjection{
		isvc:      isvc,
		key:       types.NamespacedName{Name: InferenceReplicaName(isvc.Name, v1beta1.EngineComponent), Namespace: isvc.Namespace},
		live:      live,
		liveReads: liveReads,
		writes:    writes,
	}
}

// stored reads the replica the live store holds.
func (s *twoReaderProjection) stored(t *testing.T) *v1beta1.InferenceReplica {
	t.Helper()
	ir := &v1beta1.InferenceReplica{}
	gomega.NewWithT(t).Expect(s.live.Get(context.Background(), s.key, ir)).To(gomega.Succeed())
	return ir
}

// projectThrough projects the engine with cached as the cache and the live
// store as the live reader, and returns the replica it hands back with the
// live reads and the writes it made.
func (s *twoReaderProjection) projectThrough(t *testing.T, cached client.Client) (committed *v1beta1.InferenceReplica, liveReads, writes int, err error) {
	t.Helper()
	readsBefore, writesBefore := s.liveReads.Count(), s.writes.Count()
	p := minimalParams(t, s.isvc, cached)
	p.Reader = s.live
	committed, err = EnsureInferenceReplica(context.Background(), p)
	return committed, s.liveReads.Count() - readsBefore, s.writes.Count() - writesBefore, err
}

// cachedView is the manager's client as the projector sees it: reads come
// from the copies the cache shows, writes go to the live store.
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

// cacheShowing is the manager's client with a cache that shows exactly the
// objects handed in.
func (s *twoReaderProjection) cacheShowing(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	return &cachedView{Client: s.live, shows: fakeclient.New(t, fakeclient.WithObjects(objects...))}
}

// TestEnsureInferenceReplica_ReplicaAtRestIsDecidedOnTheCachedCopy holds
// that a cached copy the projection leaves unchanged is the pass's answer:
// the live object is not read, nothing is written, and the copy is handed
// back as the committed replica.
func TestEnsureInferenceReplica_ReplicaAtRestIsDecidedOnTheCachedCopy(t *testing.T) {
	g := gomega.NewWithT(t)
	s := newTwoReaderProjection(t)
	cachedCopy := s.stored(t)

	committed, liveReads, writes, err := s.projectThrough(t, s.cacheShowing(t, cachedCopy))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(liveReads).To(gomega.Equal(0), "a replica at rest on the cached copy is not read live")
	g.Expect(writes).To(gomega.Equal(0), "a replica at rest is not written")
	g.Expect(committed.Spec).To(gomega.Equal(cachedCopy.Spec), "the cached copy is the committed replica")
	g.Expect(committed.UID).To(gomega.Equal(cachedCopy.UID))
}

// TestEnsureInferenceReplica_CachedCopyBehindOwnWriteIsReadLive holds that a
// cached copy the projection would change is never the answer: the live
// object is read, and when it already carries the projection, as after a
// write of the previous pass the cache has not shown yet, nothing is
// written.
func TestEnsureInferenceReplica_CachedCopyBehindOwnWriteIsReadLive(t *testing.T) {
	g := gomega.NewWithT(t)
	s := newTwoReaderProjection(t)
	behind := s.stored(t)
	behind.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "0"

	committed, liveReads, writes, err := s.projectThrough(t, s.cacheShowing(t, behind))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(liveReads).To(gomega.Equal(1), "a cached copy that calls for a write is decided on the live object")
	g.Expect(writes).To(gomega.Equal(0), "the live object already carries the projection")
	g.Expect(committed.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]).To(gomega.Equal("1"),
		"the committed replica is the live one")
}

// TestEnsureInferenceReplica_ChangedProjectionIsWrittenFromTheLiveObject
// holds that a projection that changed is read live and patched there, with
// the cached copy out of the write.
func TestEnsureInferenceReplica_ChangedProjectionIsWrittenFromTheLiveObject(t *testing.T) {
	g := gomega.NewWithT(t)
	s := newTwoReaderProjection(t)
	cachedCopy := s.stored(t)
	s.isvc.Generation = 2

	_, liveReads, writes, err := s.projectThrough(t, s.cacheShowing(t, cachedCopy))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(liveReads).To(gomega.Equal(1))
	g.Expect(writes).To(gomega.Equal(1))
	g.Expect(s.stored(t).Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]).To(gomega.Equal("2"),
		"the live object carries the new projection")
}

// TestEnsureInferenceReplica_CachedCopyOfAnotherOwnerIsDecidedLive holds
// that a cached copy another owner controls is not an answer either way:
// the live object is read and, being this service's, projected.
func TestEnsureInferenceReplica_CachedCopyOfAnotherOwnerIsDecidedLive(t *testing.T) {
	g := gomega.NewWithT(t)
	s := newTwoReaderProjection(t)
	other := baselineISVC("other", "prod")
	otherOwned := s.stored(t)
	otherOwned.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(other, isvcGVK)}

	committed, liveReads, writes, err := s.projectThrough(t, s.cacheShowing(t, otherOwned))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(liveReads).To(gomega.Equal(1))
	g.Expect(writes).To(gomega.Equal(0))
	g.Expect(metav1.IsControlledBy(committed, s.isvc)).To(gomega.BeTrue(), "the live object is the one this service projects")
}

// TestEnsureInferenceReplica_MissingCachedCopyIsDecidedLive holds that a
// cache that does not show the replica yet is not an answer: the live object
// is read, found, and left as it is.
func TestEnsureInferenceReplica_MissingCachedCopyIsDecidedLive(t *testing.T) {
	g := gomega.NewWithT(t)
	s := newTwoReaderProjection(t)

	_, liveReads, writes, err := s.projectThrough(t, s.cacheShowing(t))
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(liveReads).To(gomega.Equal(1))
	g.Expect(writes).To(gomega.Equal(0))
}

// TestProjectionBase_CopiesTheSpecAndMetadataAndSharesTheStatus holds that
// the base the projection is patched against is its own copy of what the
// projector rewrites and shares the rows: a change to the base's spec or
// labels leaves the object alone, and the patch rendered from the base
// carries the changed spec field and no status.
func TestProjectionBase_CopiesTheSpecAndMetadataAndSharesTheStatus(t *testing.T) {
	g := gomega.NewWithT(t)
	ir := liveIR("example", "prod", 3)
	ir.Labels = map[string]string{"role": "engine"}
	ir.Spec.Replicas = new(int32)
	*ir.Spec.Replicas = 3

	base := projectionBase(ir)
	base.Labels["role"] = "edited on the base"
	*base.Spec.Replicas = 4
	g.Expect(ir.Labels["role"]).To(gomega.Equal("engine"), "the base's labels are its own")
	g.Expect(*ir.Spec.Replicas).To(gomega.Equal(int32(3)), "the base's spec is its own")
	g.Expect(&base.Status.InstanceStatuses[0]).To(gomega.BeIdenticalTo(&ir.Status.InstanceStatuses[0]), "the rows are shared, not copied")

	data, err := client.MergeFrom(base).Data(ir)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	patch := map[string]any{}
	g.Expect(json.Unmarshal(data, &patch)).To(gomega.Succeed())
	g.Expect(patch).To(gomega.HaveKey("spec"), "the patch carries the field that differs")
	g.Expect(patch).To(gomega.HaveKey("metadata"))
	g.Expect(patch).NotTo(gomega.HaveKey("status"), "the status is never part of the patch")
}
