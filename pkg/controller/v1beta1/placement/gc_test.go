package placement

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func derivedWithOrigin(name, originUID string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod", Name: name,
		Labels:      map[string]string{PlacementOriginLabel: originUID},
		Annotations: map[string]string{PlacementOriginUIDAnnotation: originUID},
	}}
}

// derivedFromControlPlane is like derivedWithOrigin but also stamps the
// control-plane identity, as a real fan-out would.
func derivedFromControlPlane(name, originUID, controlPlaneID string) *v1beta1.InferenceService {
	d := derivedWithOrigin(name, originUID)
	d.Labels[PlacementControlPlaneLabel] = controlPlaneID
	return d
}

func TestOrphanedDeriveds(t *testing.T) {
	live := map[string]bool{"uid-live": true}
	withOrigin := func(name, originUID string) metav1.PartialObjectMetadata {
		return metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: name,
			Annotations: map[string]string{PlacementOriginUIDAnnotation: originUID},
		}}
	}
	derived := []metav1.PartialObjectMetadata{
		withOrigin("a", "uid-live"), // source exists -> keep
		withOrigin("b", "uid-gone"), // source gone -> orphan
	}
	orphans := OrphanedDeriveds(live, "", derived)
	require.Len(t, orphans, 1)
	assert.Equal(t, "b", orphans[0].Name)
}

// With a control-plane id set, OrphanedDeriveds must never
// classify a derived owned by ANOTHER control plane as an orphan, even when its
// source UID is absent from this control plane's live set.
func TestOrphanedDeriveds_ControlPlaneScoped(t *testing.T) {
	live := map[string]bool{"uid-live": true}
	withCP := func(name, originUID, cp string) metav1.PartialObjectMetadata {
		return metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: name,
			Labels:      map[string]string{PlacementControlPlaneLabel: cp},
			Annotations: map[string]string{PlacementOriginUIDAnnotation: originUID},
		}}
	}
	derived := []metav1.PartialObjectMetadata{
		withCP("mine-orphan", "uid-gone", "cp-east"),   // ours, source gone -> orphan
		withCP("theirs-orphan", "uid-gone", "cp-west"), // theirs, source gone -> NOT ours
		withCP("mine-live", "uid-live", "cp-east"),     // ours, source live -> keep
	}
	orphans := OrphanedDeriveds(live, "cp-east", derived)
	require.Len(t, orphans, 1)
	assert.Equal(t, "mine-orphan", orphans[0].Name, "must not reap another control plane's derived")
}

func TestGCSweep_DeletesOrphansOnly(t *testing.T) {
	cp := fakeclient.New(t, fakeclient.WithObjects(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "live", UID: "uid-live"},
	}))
	worker := fakeclient.New(t, fakeclient.WithObjects(
		derivedWithOrigin("live", "uid-live"),
		derivedWithOrigin("orphan", "uid-gone"),
	))
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"w": workloadcluster.NewNeverCachingClient(worker),
	}}
	gc := &GCReconciler{APIReader: cp, Log: log.Log, Clusters: clusters}

	require.NoError(t, gc.sweep(context.Background()))

	err := worker.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "orphan"}, &v1beta1.InferenceService{})
	assert.True(t, apierrors.IsNotFound(err), "orphan deleted")
	require.NoError(t, worker.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "live"}, &v1beta1.InferenceService{}), "live kept")
}

// The sweep must delete the exact object it classified, not whatever holds that
// name when the delete lands. A source deleted and recreated under the same name
// gets a fresh UID, so the placer can have a LIVE derived in place by then; a
// name-only delete would reap the workload it just placed.
func TestGCSweep_DeletesByUIDPrecondition(t *testing.T) {
	cp := fakeclient.New(t, fakeclient.WithObjects(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "live", UID: "uid-live"},
	}))
	orphan := derivedWithOrigin("orphan", "uid-gone")
	orphan.UID = "derived-uid"

	deletes := fakeclient.CountingVerb(fakeclient.Delete)
	worker := fakeclient.New(t, fakeclient.WithObjects(orphan), deletes)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"w": workloadcluster.NewNeverCachingClient(worker),
	}}
	gc := &GCReconciler{APIReader: cp, Log: log.Log, Clusters: clusters}

	require.NoError(t, gc.sweep(context.Background()))

	var uid *types.UID
	for _, o := range fakeclient.OptionsOf[client.DeleteOptions](deletes) {
		if o.Preconditions != nil {
			uid = o.Preconditions.UID
		}
	}
	require.NotNil(t, uid, "orphan delete must carry a UID precondition")
	assert.Equal(t, types.UID("derived-uid"), *uid)
}

// An empty control-plane ISVC list (transient/partial/suspect response, or a
// genuinely-empty control plane) must NOT cause the sweep to reap every
// derived: an empty live set is treated as suspect, not authoritative.
func TestGCSweep_EmptyLiveSet_NoDeletes(t *testing.T) {
	// Control plane has NO ISVCs -> live set is empty.
	cp := fakeclient.New(t)
	worker := fakeclient.New(t, fakeclient.WithObjects(
		derivedWithOrigin("a", "uid-1"),
		derivedWithOrigin("b", "uid-2"),
	))
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"w": workloadcluster.NewNeverCachingClient(worker),
	}}
	gc := &GCReconciler{APIReader: cp, Log: log.Log, Clusters: clusters}

	require.NoError(t, gc.sweep(context.Background()))

	// Both deriveds must survive: an empty live set is suspect, not authoritative.
	require.NoError(t, worker.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "a"}, &v1beta1.InferenceService{}), "a kept")
	require.NoError(t, worker.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "b"}, &v1beta1.InferenceService{}), "b kept")
}

// A source created after the sweep read the control plane and before it read
// the member reads as missing from the first list. Its derived must survive:
// the sweep confirms a missing source against a fresh read before it deletes.
func TestGCSweep_KeepsADerivedWhoseSourceAppearedDuringTheSweep(t *testing.T) {
	lists := fakeclient.CountingVerb(fakeclient.List)
	cp := fakeclient.New(t, fakeclient.WithObjects(
		&v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "keeper", UID: "uid-keeper"}},
		&v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "fresh", UID: "uid-fresh"}},
	), lists,
		// The first read of the control plane happens before the source
		// exists; every later read sees it.
		fakeclient.EditedRead(func(ml *metav1.PartialObjectMetadataList) {
			if lists.Count() != 1 {
				return
			}
			kept := ml.Items[:0]
			for _, item := range ml.Items {
				if item.UID != "uid-fresh" {
					kept = append(kept, item)
				}
			}
			ml.Items = kept
		}))
	worker := fakeclient.New(t, fakeclient.WithObjects(
		derivedWithOrigin("fresh", "uid-fresh"),
	))
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"w": workloadcluster.NewNeverCachingClient(worker),
	}}
	gc := &GCReconciler{APIReader: cp, Log: log.Log, Clusters: clusters}

	require.NoError(t, gc.sweep(context.Background()))

	require.NoError(t, worker.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "fresh"}, &v1beta1.InferenceService{}),
		"a derived whose source exists by the time the sweep reads the member must be kept")
	assert.GreaterOrEqual(t, lists.Count(), 2, "the sweep confirms a missing source with a second read")
}

// Two control planes sharing a workload cluster: each GC, scoped by its own
// ControlPlaneID, must reap ONLY its own orphans and never the other's
// deriveds — without the scoping, cp-east would reap cp-west's live derived
// because its source UID is not in cp-east's live set.
func TestGCSweep_ControlPlaneScoped_DoesNotReapOthers(t *testing.T) {
	// cp-east's control plane: source "east-live" is live; "east-gone" is gone.
	cp := fakeclient.New(t, fakeclient.WithObjects(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "east-live", UID: "uid-east-live"},
	}))
	worker := fakeclient.New(t, fakeclient.WithObjects(
		derivedFromControlPlane("east-live", "uid-east-live", "cp-east"), // ours, live -> keep
		derivedFromControlPlane("east-gone", "uid-east-gone", "cp-east"), // ours, gone -> reap
		derivedFromControlPlane("west-svc", "uid-west", "cp-west"),       // theirs -> never touch
	))
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"shared": workloadcluster.NewNeverCachingClient(worker),
	}}
	gc := &GCReconciler{APIReader: cp, Log: log.Log, Clusters: clusters, ControlPlaneID: "cp-east"}

	require.NoError(t, gc.sweep(context.Background()))

	require.NoError(t, worker.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "east-live"}, &v1beta1.InferenceService{}), "our live derived kept")
	gerr := worker.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "east-gone"}, &v1beta1.InferenceService{})
	assert.True(t, apierrors.IsNotFound(gerr), "our orphan reaped")
	require.NoError(t, worker.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "west-svc"}, &v1beta1.InferenceService{}), "other control plane's derived untouched")
}
