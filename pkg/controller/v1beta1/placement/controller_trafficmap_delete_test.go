package placement

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

func TestReconcileDelete_WaitsForEndpointCleanupBeforeDerived(t *testing.T) {
	scheme := testScheme(t)
	worker := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc",
			Namespace: "prod",
			Labels:    map[string]string{PlacementOriginLabel: "uid-1"},
		},
	}).Build()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(worker),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer, EndpointFinalizer}
	r, controlPlane := newPlacer(scheme, clusters, isvc)
	require.NoError(t, controlPlane.Delete(context.Background(), isvc))

	result, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Positive(t, result.RequeueAfter)
	assert.True(t, hasDerived(t, worker), "backend must remain while endpoint cleanup is pending")
	liveISVC := &v1beta1.InferenceService{}
	require.NoError(t, controlPlane.Get(context.Background(), req().NamespacedName, liveISVC))
	assert.Contains(t, liveISVC.Finalizers, PlacementFinalizer)
	assert.Contains(t, liveISVC.Finalizers, EndpointFinalizer)

	// The endpoint controller owns and releases its finalizer independently.
	liveISVC.Finalizers = slices.DeleteFunc(liveISVC.Finalizers, func(finalizer string) bool {
		return finalizer == EndpointFinalizer
	})
	require.NoError(t, controlPlane.Update(context.Background(), liveISVC))

	_, err = r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.False(t, hasDerived(t, worker), "backend teardown may proceed after endpoint cleanup")
	err = controlPlane.Get(context.Background(), req().NamespacedName, &v1beta1.InferenceService{})
	assert.True(t, apierrors.IsNotFound(err), "source may disappear after both finalizers are released")
}

func TestReconcileDelete_WaitsForOrphanedTrafficMapBeforeDerived(t *testing.T) {
	scheme := testScheme(t)
	worker := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc",
			Namespace: "prod",
			Labels:    map[string]string{PlacementOriginLabel: "uid-1"},
		},
	}).Build()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(worker),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	claims := []string{"v1:httproute:prod/svc-global"}
	tm := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:       isvc.Name,
			Namespace:  isvc.Namespace,
			UID:        "traffic-map-uid",
			Finalizers: []string{TrafficMapPublisherFinalizer},
		},
		Status: v1beta1.TrafficMapStatus{
			SourceUID: isvc.UID,
			Publisher: &v1beta1.TrafficMapPublisherStatus{
				PublisherName:  "gatewayapi",
				ClaimedTargets: claims,
			},
		},
	}
	r, controlPlane := newPlacer(scheme, clusters, isvc, tm)
	require.NoError(t, controlPlane.Delete(context.Background(), isvc))
	require.NoError(t, controlPlane.Delete(context.Background(), tm))

	result, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Positive(t, result.RequeueAfter)
	assert.True(t, hasDerived(t, worker), "backend must remain while the orphaned TrafficMap is terminating")
	liveISVC := &v1beta1.InferenceService{}
	require.NoError(t, controlPlane.Get(context.Background(), req().NamespacedName, liveISVC))
	assert.Contains(t, liveISVC.Finalizers, PlacementFinalizer)
	liveMap := &v1beta1.TrafficMap{}
	require.NoError(t, controlPlane.Get(context.Background(), req().NamespacedName, liveMap))
	require.NotNil(t, liveMap.DeletionTimestamp)
	require.NotNil(t, liveMap.Status.Publisher)
	assert.Equal(t, claims, liveMap.Status.Publisher.ClaimedTargets)

	liveMap.Finalizers = nil
	require.NoError(t, controlPlane.Update(context.Background(), liveMap))
	if err := controlPlane.Get(context.Background(), req().NamespacedName, &v1beta1.TrafficMap{}); err == nil {
		require.NoError(t, controlPlane.Delete(context.Background(), liveMap))
	} else {
		require.True(t, apierrors.IsNotFound(err))
	}

	_, err = r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.False(t, hasDerived(t, worker), "backend teardown may proceed after the owned TrafficMap disappears")
	err = controlPlane.Get(context.Background(), req().NamespacedName, &v1beta1.InferenceService{})
	assert.True(t, apierrors.IsNotFound(err), "source may disappear after the placement finalizer is released")
}

func TestTrafficMapBlocksSourceTeardownIdentityMatrix(t *testing.T) {
	source := srcISVC("gpu=gb300")
	exactOwner := func() metav1.OwnerReference {
		return *metav1.NewControllerRef(source, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))
	}
	conflictingOwner := func() metav1.OwnerReference {
		owner := exactOwner()
		owner.UID = "other-source-uid"
		return owner
	}
	tests := []struct {
		name      string
		configure func(*v1beta1.TrafficMap)
		wantBlock bool
	}{
		{
			name: "exact owner with legacy empty provenance",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.OwnerReferences = []metav1.OwnerReference{exactOwner()}
			},
			wantBlock: true,
		},
		{
			name: "exact owner with matching provenance",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.OwnerReferences = []metav1.OwnerReference{exactOwner()}
				tm.Status.SourceUID = source.UID
			},
			wantBlock: true,
		},
		{
			name: "exact owner with mismatched provenance is conflict",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.OwnerReferences = []metav1.OwnerReference{exactOwner()}
				tm.Status.SourceUID = "other-source-uid"
			},
			wantBlock: true,
		},
		{
			name: "ownerless matching provenance",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.Status.SourceUID = source.UID
			},
			wantBlock: true,
		},
		{
			name: "non-controller owner with matching provenance",
			configure: func(tm *v1beta1.TrafficMap) {
				owner := conflictingOwner()
				owner.Controller = nil
				tm.OwnerReferences = []metav1.OwnerReference{owner}
				tm.Status.SourceUID = source.UID
			},
			wantBlock: true,
		},
		{
			name: "conflicting controller with matching provenance is conflict",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.OwnerReferences = []metav1.OwnerReference{conflictingOwner()}
				tm.Status.SourceUID = source.UID
			},
			wantBlock: true,
		},
		{
			name: "hidden conflicting controller prevents exact ownership",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.OwnerReferences = []metav1.OwnerReference{exactOwner(), conflictingOwner()}
			},
		},
		{
			name: "ownerless empty provenance with publisher finalizer",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.Finalizers = []string{TrafficMapPublisherFinalizer}
			},
			wantBlock: true,
		},
		{
			name: "ownerless empty provenance with publisher status",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{PublisherName: "gatewayapi"}
			},
			wantBlock: true,
		},
		{
			name: "ownerless empty provenance with claims",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
					PublisherName: "gatewayapi", ClaimedTargets: []string{"target"},
				}
			},
			wantBlock: true,
		},
		{
			name: "ownerless empty provenance with published bit",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.Status.Published = true
			},
			wantBlock: true,
		},
		{
			name: "ownerless empty provenance with gateway reference",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.Status.GatewayRef = &v1beta1.TrafficMapGatewayRef{Name: "route"}
			},
			wantBlock: true,
		},
		{
			name: "ownerless empty provenance with observed generation",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.Status.ObservedTrafficMapGeneration = 1
			},
			wantBlock: true,
		},
		{
			name: "ownerless empty provenance with published condition",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.Status.Conditions = []metav1.Condition{{
					Type: v1beta1.TrafficMapPublished, Status: metav1.ConditionFalse,
					Reason: "Pending", LastTransitionTime: metav1.Now(),
				}}
			},
			wantBlock: true,
		},
		{
			name:      "ownerless empty provenance without publication evidence",
			configure: func(*v1beta1.TrafficMap) {},
		},
		{
			name: "ownerless mismatched provenance with publication evidence is foreign",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.Status.SourceUID = "other-source-uid"
				tm.Finalizers = []string{TrafficMapPublisherFinalizer}
				tm.Status.Published = true
			},
		},
		{
			name: "conflicting controller with empty provenance is foreign",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.OwnerReferences = []metav1.OwnerReference{conflictingOwner()}
				tm.Finalizers = []string{TrafficMapPublisherFinalizer}
			},
		},
		{
			name: "conflicting controller with mismatched provenance is foreign",
			configure: func(tm *v1beta1.TrafficMap) {
				tm.OwnerReferences = []metav1.OwnerReference{conflictingOwner()}
				tm.Status.SourceUID = "third-source-uid"
				tm.Status.Published = true
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tm := &v1beta1.TrafficMap{ObjectMeta: metav1.ObjectMeta{
				Name: source.Name, Namespace: source.Namespace,
			}}
			tc.configure(tm)
			assert.Equal(t, tc.wantBlock, trafficMapBlocksSourceTeardown(tm, source))
		})
	}
}

func TestReconcileDelete_TrafficMapIdentityControlsBackendTeardown(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*v1beta1.TrafficMap, *v1beta1.InferenceService)
		wantBlock bool
	}{
		{
			name: "ambiguous stateless publication",
			configure: func(tm *v1beta1.TrafficMap, _ *v1beta1.InferenceService) {
				tm.Status.Published = true
				tm.Status.Conditions = []metav1.Condition{{
					Type: v1beta1.TrafficMapPublished, Status: metav1.ConditionTrue,
					Reason: "Published", LastTransitionTime: metav1.Now(),
				}}
			},
			wantBlock: true,
		},
		{
			name: "conflicting controller with matching provenance",
			configure: func(tm *v1beta1.TrafficMap, source *v1beta1.InferenceService) {
				owner := *metav1.NewControllerRef(source,
					v1beta1.SchemeGroupVersion.WithKind("InferenceService"))
				owner.UID = "other-source-uid"
				tm.OwnerReferences = []metav1.OwnerReference{owner}
				tm.Status.SourceUID = source.UID
			},
			wantBlock: true,
		},
		{
			name: "ownerless mismatched provenance is foreign despite publication evidence",
			configure: func(tm *v1beta1.TrafficMap, _ *v1beta1.InferenceService) {
				tm.Status.SourceUID = "other-source-uid"
				tm.Status.Published = true
				tm.Finalizers = []string{TrafficMapPublisherFinalizer}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scheme := testScheme(t)
			worker := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(&v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name: "svc", Namespace: "prod",
					Labels: map[string]string{PlacementOriginLabel: "uid-1"},
				},
			}).Build()
			clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
				"a": workloadcluster.NewNeverCachingClient(worker),
			}}
			source := srcISVC("gpu=gb300")
			source.Finalizers = []string{PlacementFinalizer}
			tm := &v1beta1.TrafficMap{ObjectMeta: metav1.ObjectMeta{
				Name: source.Name, Namespace: source.Namespace, UID: "traffic-map-uid",
			}}
			tc.configure(tm, source)
			r, controlPlane := newPlacer(scheme, clusters, source, tm)
			require.NoError(t, controlPlane.Delete(context.Background(), source))

			result, err := r.Reconcile(context.Background(), req())

			require.NoError(t, err)
			assert.Equal(t, tc.wantBlock, result.RequeueAfter > 0)
			assert.Equal(t, tc.wantBlock, hasDerived(t, worker))
			liveSource := &v1beta1.InferenceService{}
			err = controlPlane.Get(context.Background(), req().NamespacedName, liveSource)
			if tc.wantBlock {
				require.NoError(t, err)
				assert.Contains(t, liveSource.Finalizers, PlacementFinalizer)
			} else {
				assert.True(t, apierrors.IsNotFound(err))
			}
		})
	}
}

type trafficMapGetErrorReader struct {
	client.Reader
	err error
}

func (r trafficMapGetErrorReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	opts ...client.GetOption,
) error {
	if _, ok := object.(*v1beta1.TrafficMap); ok {
		return r.err
	}
	return r.Reader.Get(ctx, key, object, opts...)
}

func TestReconcileDelete_TrafficMapReadFailureKeepsDerived(t *testing.T) {
	scheme := testScheme(t)
	worker := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc",
			Namespace: "prod",
			Labels:    map[string]string{PlacementOriginLabel: "uid-1"},
		},
	}).Build()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(worker),
	}}
	isvc := srcISVC("gpu=gb300")
	isvc.Finalizers = []string{PlacementFinalizer}
	r, controlPlane := newPlacer(scheme, clusters, isvc)
	require.NoError(t, controlPlane.Delete(context.Background(), isvc))
	r.APIReader = trafficMapGetErrorReader{
		Reader: controlPlane,
		err:    apierrors.NewInternalError(errors.New("TrafficMap read failed")),
	}

	_, err := r.Reconcile(context.Background(), req())

	require.ErrorContains(t, err, "check TrafficMap teardown barrier")
	assert.True(t, hasDerived(t, worker), "an uncertain barrier must fail closed before backend teardown")
	liveISVC := &v1beta1.InferenceService{}
	require.NoError(t, controlPlane.Get(context.Background(), req().NamespacedName, liveISVC))
	assert.Contains(t, liveISVC.Finalizers, PlacementFinalizer)
}

func TestReconcileDelete_UsesAuthoritativeReaderForOrphanBarrier(t *testing.T) {
	scheme := testScheme(t)
	worker := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "prod",
			Labels: map[string]string{PlacementOriginLabel: "uid-1"},
		},
	}).Build()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(worker),
	}}
	source := srcISVC("gpu=gb300")
	source.Finalizers = []string{PlacementFinalizer}
	r, cacheClient := newPlacer(scheme, clusters, source)
	liveMap := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: source.Name, Namespace: source.Namespace, UID: "traffic-map-uid",
		},
		Status: v1beta1.TrafficMapStatus{SourceUID: source.UID},
	}
	r.APIReader = fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(liveMap).Build()
	require.NoError(t, cacheClient.Delete(context.Background(), source))
	require.True(t, apierrors.IsNotFound(cacheClient.Get(
		context.Background(), req().NamespacedName, &v1beta1.TrafficMap{},
	)), "the cached client intentionally does not contain the live TrafficMap")

	result, err := r.Reconcile(context.Background(), req())

	require.NoError(t, err)
	assert.Positive(t, result.RequeueAfter)
	assert.True(t, hasDerived(t, worker))
	liveSource := &v1beta1.InferenceService{}
	require.NoError(t, cacheClient.Get(context.Background(), req().NamespacedName, liveSource))
	assert.Contains(t, liveSource.Finalizers, PlacementFinalizer)
}

func TestReconcileDelete_MissingAPIReaderKeepsDerived(t *testing.T) {
	scheme := testScheme(t)
	worker := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "prod",
			Labels: map[string]string{PlacementOriginLabel: "uid-1"},
		},
	}).Build()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(worker),
	}}
	source := srcISVC("gpu=gb300")
	source.Finalizers = []string{PlacementFinalizer}
	r, controlPlane := newPlacer(scheme, clusters, source)
	r.APIReader = nil
	require.NoError(t, controlPlane.Delete(context.Background(), source))

	_, err := r.Reconcile(context.Background(), req())

	require.ErrorContains(t, err, "API reader is not configured")
	assert.True(t, hasDerived(t, worker))
	liveSource := &v1beta1.InferenceService{}
	require.NoError(t, controlPlane.Get(context.Background(), req().NamespacedName, liveSource))
	assert.Contains(t, liveSource.Finalizers, PlacementFinalizer)
}

func TestReconcileDelete_ForeignOrMalformedTrafficMapDoesNotBlock(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*metav1.OwnerReference)
	}{
		{name: "different UID", mutate: func(owner *metav1.OwnerReference) { owner.UID = "other-source-uid" }},
		{name: "different API version", mutate: func(owner *metav1.OwnerReference) { owner.APIVersion = "other.example/v1" }},
		{name: "different kind", mutate: func(owner *metav1.OwnerReference) { owner.Kind = "Other" }},
		{name: "different name", mutate: func(owner *metav1.OwnerReference) { owner.Name = "other" }},
		{name: "not controller", mutate: func(owner *metav1.OwnerReference) { owner.Controller = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := testScheme(t)
			worker := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(&v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "svc",
					Namespace: "prod",
					Labels:    map[string]string{PlacementOriginLabel: "uid-1"},
				},
			}).Build()
			clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
				"a": workloadcluster.NewNeverCachingClient(worker),
			}}
			isvc := srcISVC("gpu=gb300")
			isvc.Finalizers = []string{PlacementFinalizer}
			owner := metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))
			tc.mutate(owner)
			foreign := &v1beta1.TrafficMap{ObjectMeta: metav1.ObjectMeta{
				Name:            isvc.Name,
				Namespace:       isvc.Namespace,
				UID:             "foreign-map-uid",
				OwnerReferences: []metav1.OwnerReference{*owner},
			}}
			r, controlPlane := newPlacer(scheme, clusters, isvc, foreign)
			require.NoError(t, controlPlane.Delete(context.Background(), isvc))

			_, err := r.Reconcile(context.Background(), req())

			require.NoError(t, err)
			assert.False(t, hasDerived(t, worker))
			preserved := &v1beta1.TrafficMap{}
			require.NoError(t, controlPlane.Get(context.Background(), req().NamespacedName, preserved))
			assert.Equal(t, types.UID("foreign-map-uid"), preserved.UID)
		})
	}
}
