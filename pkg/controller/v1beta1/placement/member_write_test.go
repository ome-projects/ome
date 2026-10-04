package placement

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
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

func memberWorker(s *runtime.Scheme, funcs interceptor.Funcs, objs ...client.Object) client.WithWatch {
	objs = append(objs, backendTestRuntime())
	base := fakeclient.NewClientBuilder().WithScheme(s).WithStatusSubresource(&v1beta1.InferenceService{}).WithObjects(objs...).Build()
	return interceptor.NewClient(base, funcs)
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
	worker := memberWorker(s, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if err := c.Get(ctx, key, obj, opts...); err != nil {
			return err
		}
		if _, runtimeRead := obj.(*v1beta1.ClusterServingRuntime); runtimeRead {
			bumpMemberStatus(ctx, t, c)
		}
		return nil
	}}, ownedMember())
	r := placerFor(t, s, workloadcluster.NewNeverCachingClient(worker))

	require.NoError(t, r.placeOn(t.Context(), "a", srcISVC(""), false))

	got := liveMember(t, worker)
	assert.NotNil(t, got.Spec.Engine, "desired spec applied")
	assert.Equal(t, "uid-1", got.Labels[PlacementOriginLabel])
	assert.Equal(t, "yes", got.Labels["worker.io/managed"], "worker label preserved")
}

// A write that loses to a concurrent member write is retried from a fresh read.
func TestPlaceOnRetriesMemberConflict(t *testing.T) {
	s := testScheme(t)
	updates := 0
	worker := memberWorker(s, interceptor.Funcs{Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		updates++
		if updates == 1 {
			bumpMemberStatus(ctx, t, c)
		}
		return c.Update(ctx, obj, opts...)
	}}, ownedMember())
	r := placerFor(t, s, workloadcluster.NewNeverCachingClient(worker))

	require.NoError(t, r.placeOn(t.Context(), "a", srcISVC(""), false))

	assert.Equal(t, 2, updates, "one conflicted write and one retry")
	got := liveMember(t, worker)
	assert.NotNil(t, got.Spec.Engine, "desired spec applied")
	assert.Equal(t, "uid-1", got.Labels[PlacementOriginLabel])
	assert.Equal(t, "yes", got.Labels["worker.io/managed"], "worker label preserved")
	assert.Equal(t, int64(1), got.Status.ObservedGeneration, "concurrent status write preserved")
}

// Each retry repeats the ownership checks against the object it would replace,
// so a member that changes hands between attempts is never overwritten.
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
			worker := memberWorker(s, interceptor.Funcs{
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
			}, tt.initial...)
			r := placerFor(t, s, workloadcluster.NewNeverCachingClient(worker))

			err := r.placeOn(t.Context(), "a", srcISVC(""), false)
			require.ErrorContains(t, err, tt.wantErr)
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
	if member, ok := obj.(*v1beta1.InferenceService); ok && key == memberKey {
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

// The member write reads through the direct client, so a cache that lags the
// member cannot pin the write to a stale resourceVersion.
func TestPlaceOnIgnoresStaleMemberCache(t *testing.T) {
	s := testScheme(t)
	worker := memberWorker(s, interceptor.Funcs{}, ownedMember())
	stale := liveMember(t, worker)
	bumpMemberStatus(t.Context(), t, worker)
	r := placerFor(t, s, &staleCacheClient{WithWatch: worker, stale: stale})

	require.NoError(t, r.placeOn(t.Context(), "a", srcISVC(""), false))

	assert.NotNil(t, liveMember(t, worker).Spec.Engine, "desired spec applied")
}
