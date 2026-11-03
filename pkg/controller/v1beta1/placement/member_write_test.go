package placement

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var memberKey = types.NamespacedName{Namespace: "prod", Name: "svc"}

// ownedMember is a standing derived of srcISVC with a worker-added label.
func ownedMember() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: memberKey.Name, Namespace: memberKey.Namespace, UID: "member-uid",
		Labels: map[string]string{"worker.io/managed": "yes", PlacementOriginLabel: "uid-1"},
	}}
}

// foreignMember is a same-named service no placement source derived.
func foreignMember(uid types.UID) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: memberKey.Name, Namespace: memberKey.Namespace, UID: uid,
			Labels: map[string]string{"owner": "some-user"},
		},
		Spec: v1beta1.InferenceServiceSpec{
			Router: &v1beta1.RouterSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "user-container", Image: "user-img"}}},
		},
	}
}

// memberWorker is a member cluster's client: its store holds the backend
// runtime beside the objects an option seeds, under the behaviours and raw
// hooks the test lays over it.
func memberWorker(t *testing.T, s *runtime.Scheme, opts ...fakeclient.Option) client.WithWatch {
	t.Helper()
	return fakeclient.New(t, append([]fakeclient.Option{
		fakeclient.WithScheme(s),
		fakeclient.WithStatusSubresource(&v1beta1.InferenceService{}),
		fakeclient.WithObjects(backendTestRuntime()),
	}, opts...)...)
}

// bumpMemberStatus models the member reconciler's status write: it advances
// the resourceVersion without touching spec or metadata.
func bumpMemberStatus(ctx context.Context, t *testing.T, c client.Client) {
	t.Helper()
	cur := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(ctx, memberKey, cur))
	cur.Status.ObservedGeneration++
	require.NoError(t, c.Status().Update(ctx, cur))
}

func placerFor(t *testing.T, s *runtime.Scheme, member workloadcluster.SelectivelyCachingClient) *Reconciler {
	t.Helper()
	r, _ := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"a": member}}, srcISVC(""), readyWC("a", nil))
	return r
}

func liveMember(t *testing.T, c client.Client) *v1beta1.InferenceService {
	t.Helper()
	got := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), memberKey, got))
	return got
}

// A member whose status is rewritten while the placer verifies remote inputs
// must still accept the placer's write.
func TestPlaceOnWritesThroughMemberStatusChurn(t *testing.T) {
	s := testScheme(t)
	worker := memberWorker(t, s, fakeclient.WithObjects(ownedMember()), fakeclient.WithInterceptors(interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if err := c.Get(ctx, key, obj, opts...); err != nil {
			return err
		}
		if _, runtimeRead := obj.(*v1beta1.ClusterServingRuntime); runtimeRead {
			bumpMemberStatus(ctx, t, c)
		}
		return nil
	}}))
	r := placerFor(t, s, workloadcluster.NewNeverCachingClient(worker))

	require.NoError(t, r.placeOn(t.Context(), "a", srcISVC(""), false))

	got := liveMember(t, worker)
	assert.NotNil(t, got.Spec.Engine, "desired spec applied")
	assert.Equal(t, "uid-1", got.Labels[PlacementOriginLabel])
	assert.Equal(t, "yes", got.Labels["worker.io/managed"], "worker label preserved")
}

// A conflict returns to reconciliation before attempting another member write.
func TestPlaceOnRetriesMemberConflictOnNextReconcile(t *testing.T) {
	s := testScheme(t)
	updates := fakeclient.CountingVerb(fakeclient.Update)
	worker := memberWorker(t, s, fakeclient.WithObjects(ownedMember()), updates, fakeclient.WithInterceptors(interceptor.Funcs{Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if updates.Count() == 1 {
			bumpMemberStatus(ctx, t, c)
		}
		return c.Update(ctx, obj, opts...)
	}}))
	r := placerFor(t, s, workloadcluster.NewNeverCachingClient(worker))

	require.True(t, apierrors.IsConflict(r.placeOn(t.Context(), "a", srcISVC(""), false)))
	assert.Equal(t, 1, updates.Count(), "a conflict must not loop over the same cached state")
	require.NoError(t, r.placeOn(t.Context(), "a", srcISVC(""), false))

	assert.Equal(t, 2, updates.Count(), "one conflicted write and one subsequent reconcile")
	got := liveMember(t, worker)
	assert.NotNil(t, got.Spec.Engine, "desired spec applied")
	assert.Equal(t, "uid-1", got.Labels[PlacementOriginLabel])
	assert.Equal(t, "yes", got.Labels["worker.io/managed"], "worker label preserved")
	assert.Equal(t, int64(1), got.Status.ObservedGeneration, "concurrent status write preserved")
}

// Each reconciliation checks ownership before replacing a member.
func TestPlaceOnRetryRechecksMemberOwnership(t *testing.T) {
	for _, tt := range []struct {
		name    string
		initial []client.Object
		race    func(context.Context, *testing.T, client.WithWatch)
		wantErr string
		want    func(*testing.T, *v1beta1.InferenceService)
	}{
		{
			name:    "origin markers removed",
			initial: []client.Object{ownedMember()},
			race: func(ctx context.Context, t *testing.T, c client.WithWatch) {
				cur := &v1beta1.InferenceService{}
				require.NoError(t, c.Get(ctx, memberKey, cur))
				delete(cur.Labels, PlacementOriginLabel)
				require.NoError(t, c.Update(ctx, cur))
			},
			wantErr: "refusing to overwrite non-derived InferenceService",
			want: func(t *testing.T, got *v1beta1.InferenceService) {
				assert.Empty(t, got.Labels[PlacementOriginLabel], "origin label not restamped")
				assert.Nil(t, got.Spec.Engine, "spec not replaced")
			},
		},
		{
			name:    "replaced by a foreign service",
			initial: []client.Object{ownedMember()},
			race: func(ctx context.Context, t *testing.T, c client.WithWatch) {
				require.NoError(t, c.Delete(ctx, ownedMember()))
				require.NoError(t, c.Create(ctx, foreignMember("foreign-uid")))
			},
			wantErr: "refusing to overwrite non-derived InferenceService",
			want: func(t *testing.T, got *v1beta1.InferenceService) {
				assert.Equal(t, types.UID("foreign-uid"), got.UID)
				assert.Empty(t, got.Labels[PlacementOriginLabel], "origin label not stamped")
				assert.NotNil(t, got.Spec.Router, "foreign spec preserved")
				assert.Nil(t, got.Spec.Engine, "foreign spec preserved")
			},
		},
		{
			name:    "replaced by another derived instance",
			initial: []client.Object{ownedMember()},
			race: func(ctx context.Context, t *testing.T, c client.WithWatch) {
				require.NoError(t, c.Delete(ctx, ownedMember()))
				replacement := ownedMember()
				replacement.UID = "replacement-uid"
				require.NoError(t, c.Create(ctx, replacement))
			},
			wantErr: "replaced after backend verification",
			want: func(t *testing.T, got *v1beta1.InferenceService) {
				assert.Equal(t, types.UID("replacement-uid"), got.UID)
				assert.Nil(t, got.Spec.Engine, "unverified instance not written")
			},
		},
		{
			name: "foreign service created concurrently",
			race: func(ctx context.Context, t *testing.T, c client.WithWatch) {
				require.NoError(t, c.Create(ctx, foreignMember("foreign-uid")))
			},
			wantErr: "refusing to overwrite non-derived InferenceService",
			want: func(t *testing.T, got *v1beta1.InferenceService) {
				assert.Equal(t, types.UID("foreign-uid"), got.UID)
				assert.Nil(t, got.Spec.Engine, "foreign spec preserved")
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := testScheme(t)
			raced := false
			race := func(ctx context.Context, c client.WithWatch) {
				if !raced {
					raced = true
					tt.race(ctx, t, c)
				}
			}
			worker := memberWorker(t, s, fakeclient.WithObjects(tt.initial...), fakeclient.WithInterceptors(interceptor.Funcs{
				Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					race(ctx, c)
					return c.Update(ctx, obj, opts...)
				},
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if _, member := obj.(*v1beta1.InferenceService); member {
						race(ctx, c)
					}
					return c.Create(ctx, obj, opts...)
				},
			}))
			r := placerFor(t, s, workloadcluster.NewNeverCachingClient(worker))

			err := r.placeOn(t.Context(), "a", srcISVC(""), false)
			require.True(t, isMemberWriteRace(err), "the API must reject the concurrent replacement: %v", err)
			if tt.name != "replaced by another derived instance" {
				require.ErrorContains(t, r.placeOn(t.Context(), "a", srcISVC(""), false), tt.wantErr)
			}
			require.True(t, raced, "the write raced the ownership change")
			tt.want(t, liveMember(t, worker))
		})
	}
}

// staleCacheClient serves a fixed stale member service from its cached reads
// while its direct client reaches the live object.
type staleCacheClient struct {
	client.WithWatch
	stale *v1beta1.InferenceService
}

func (c *staleCacheClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if member, ok := obj.(*v1beta1.InferenceService); ok && (c.stale == nil || key == memberKey) {
		if c.stale == nil {
			return apierrors.NewNotFound(schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}, key.Name)
		}
		c.stale.DeepCopyInto(member)
		return nil
	}
	return c.WithWatch.Get(ctx, key, obj, opts...)
}

func (c *staleCacheClient) AddCacheEventHandler(context.Context, client.Object, toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (c *staleCacheClient) DirectClient() client.WithWatch {
	return c.WithWatch
}

// Stale observations may reject a write, but cannot overwrite newer member state.
func TestPlaceOnWaitsForMemberCacheAfterConflict(t *testing.T) {
	s := testScheme(t)
	worker := memberWorker(t, s, fakeclient.WithObjects(ownedMember()))
	cached := &staleCacheClient{WithWatch: worker, stale: liveMember(t, worker)}
	bumpMemberStatus(t.Context(), t, worker)
	r := placerFor(t, s, cached)

	require.True(t, apierrors.IsConflict(r.placeOn(t.Context(), "a", srcISVC(""), false)))
	assert.Nil(t, liveMember(t, worker).Spec.Engine)
	cached.stale = liveMember(t, worker)
	require.NoError(t, r.placeOn(t.Context(), "a", srcISVC(""), false))
	assert.NotNil(t, liveMember(t, worker).Spec.Engine)
}

func TestPolicyPruningUsesSuccessfulWriteResponse(t *testing.T) {
	for _, pruned := range []bool{false, true} {
		t.Run(fmt.Sprintf("pruned=%t", pruned), func(t *testing.T) {
			s := testScheme(t)
			worker := memberWorker(t, s, fakeclient.WithObjects(ownedMember()), fakeclient.WithInterceptors(interceptor.Funcs{Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if pruned {
					obj.(*v1beta1.InferenceService).Spec.Engine.AutoscalerPolicyRef = nil
				}
				return c.Update(ctx, obj, opts...)
			}}))
			cached := &staleCacheClient{WithWatch: worker, stale: liveMember(t, worker)}
			src := srcISVCWithRef("")
			r, _ := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"a": cached}}, src, readyWC("a", nil))
			require.NoError(t, r.placeOn(t.Context(), "a", src, false))
			for range policyPruneRevertThreshold {
				require.True(t, apierrors.IsConflict(r.placeOn(t.Context(), "a", src, false)))
			}
			require.Empty(t, r.policyState().prunedMembers(src.UID), "stale cache retries cannot establish repeated pruning")
			for range policyPruneRevertThreshold {
				cached.stale = liveMember(t, worker)
				require.NoError(t, r.placeOn(t.Context(), "a", src, false))
			}
			if pruned {
				require.Equal(t, []string{"a"}, r.policyState().prunedMembers(src.UID))
			} else {
				require.Empty(t, r.policyState().prunedMembers(src.UID))
			}
		})
	}
}

func TestCachedAbsenceCannotReleaseUnobservedMember(t *testing.T) {
	for _, state := range []string{"exists", "absent", "unreadable"} {
		t.Run(state, func(t *testing.T) {
			s := testScheme(t)
			fixture := observationFixture(t)
			var objects []client.Object
			if state == "exists" {
				objects = append(objects, fixture.member)
			}
			gets := fakeclient.CountingVerb(fakeclient.Get).OnKind(&v1beta1.InferenceService{})
			opts := []fakeclient.Option{fakeclient.WithObjects(objects...), gets}
			if state == "unreadable" {
				opts = append(opts, fakeclient.FailingVerb(fakeclient.Get, fakeclient.EveryCall, context.DeadlineExceeded).OnKind(&v1beta1.InferenceService{}))
			}
			worker := memberWorker(t, s, opts...)
			cached := &staleCacheClient{WithWatch: worker}
			clusters := identifiedTestClusters{fakeClusters: fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"member-a": cached}}, uid: "member-a-uid"}
			r, _ := newPlacer(s, clusters, fixture.source, plannedTestRegistration())
			got, err := r.observePlannedMember(t.Context(), fixture.source, fixture.source.Status.Placement.Candidates[0], fixture.components)
			require.Equal(t, 1, gets.Count(), "allocation release requires one direct absence confirmation")
			if state == "absent" {
				require.NoError(t, err)
				require.True(t, got.MemberState.Absent)
			} else {
				require.Error(t, err)
				require.False(t, got.MemberState.Known)
				require.False(t, got.MemberState.Absent)
			}
		})
	}
}
