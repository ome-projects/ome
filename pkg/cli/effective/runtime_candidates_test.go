package effective

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/utils/ptr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

type runtimeCandidateListClient struct {
	ctrlclient.Client
	list func(context.Context, ctrlclient.ObjectList, ...ctrlclient.ListOption) error
	get  func(context.Context, ctrlclient.ObjectKey, ctrlclient.Object, ...ctrlclient.GetOption) error
}

func (c *runtimeCandidateListClient) Get(
	ctx context.Context,
	key ctrlclient.ObjectKey,
	object ctrlclient.Object,
	opts ...ctrlclient.GetOption,
) error {
	if c.get != nil {
		return c.get(ctx, key, object, opts...)
	}
	return c.Client.Get(ctx, key, object, opts...)
}

func (c *runtimeCandidateListClient) List(
	ctx context.Context,
	list ctrlclient.ObjectList,
	opts ...ctrlclient.ListOption,
) error {
	if c.list != nil {
		return c.list(ctx, list, opts...)
	}
	return c.Client.List(ctx, list, opts...)
}

type runtimeCandidateRequest struct {
	kind            string
	namespace       string
	labelSelector   string
	fieldSelector   string
	resourceVersion string
	limit           int64
	continueToken   string
	deadline        time.Time
}

func recordRuntimeCandidateRequest(
	ctx context.Context,
	list ctrlclient.ObjectList,
	opts ...ctrlclient.ListOption,
) runtimeCandidateRequest {
	options := (&ctrlclient.ListOptions{}).ApplyOptions(opts)
	raw := options.AsListOptions()
	deadline, _ := ctx.Deadline()
	kind := "other"
	switch list.(type) {
	case *v1beta1.ServingRuntimeList:
		kind = "ServingRuntime"
	case *v1beta1.ClusterServingRuntimeList:
		kind = "ClusterServingRuntime"
	}
	return runtimeCandidateRequest{
		kind:            kind,
		namespace:       options.Namespace,
		labelSelector:   raw.LabelSelector,
		fieldSelector:   raw.FieldSelector,
		resourceVersion: raw.ResourceVersion,
		limit:           raw.Limit,
		continueToken:   raw.Continue,
		deadline:        deadline,
	}
}

func candidateLimits() paging.Limits {
	return paging.Limits{
		PageSize:       2,
		MaxItems:       5,
		MaxPages:       3,
		RequestTimeout: time.Second,
	}
}

func newRuntimeCandidateBaseClient(t *testing.T, objects ...ctrlclient.Object) ctrlclient.Client {
	t.Helper()
	return ctrlclientfake.NewClientBuilder().
		WithScheme(targetScheme(t)).
		WithObjects(objects...).
		Build()
}

func TestBoundedRuntimeResolverPagesCandidateListsUnderOneBudget(t *testing.T) {
	t.Parallel()

	var requests []runtimeCandidateRequest
	base := newRuntimeCandidateBaseClient(t)
	client := &runtimeCandidateListClient{Client: base}
	client.list = func(ctx context.Context, list ctrlclient.ObjectList, opts ...ctrlclient.ListOption) error {
		request := recordRuntimeCandidateRequest(ctx, list, opts...)
		requests = append(requests, request)
		switch typed := list.(type) {
		case *v1beta1.ServingRuntimeList:
			switch request.continueToken {
			case "":
				typed.Items = []v1beta1.ServingRuntime{
					{ObjectMeta: metav1.ObjectMeta{Name: "runtime-a", Namespace: "workloads"}},
					{ObjectMeta: metav1.ObjectMeta{Name: "runtime-b", Namespace: "workloads"}},
				}
				typed.Continue = "next-runtime-page"
			case "next-runtime-page":
				typed.Items = []v1beta1.ServingRuntime{
					{ObjectMeta: metav1.ObjectMeta{Name: "runtime-c", Namespace: "workloads"}},
				}
			default:
				return fmt.Errorf("unexpected ServingRuntime continue token %q", request.continueToken)
			}
		case *v1beta1.ClusterServingRuntimeList:
			typed.Items = []v1beta1.ClusterServingRuntime{
				{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "cluster-b"}},
			}
		default:
			return fmt.Errorf("unexpected list type %T", list)
		}
		return nil
	}

	resolver, err := NewBoundedRuntimeResolver(client, candidateLimits())
	require.NoError(t, err)

	raw := &metav1.ListOptions{ResourceVersion: "resource-version-7"}
	callerOptions := &ctrlclient.ListOptions{
		Namespace:     "workloads",
		LabelSelector: labels.SelectorFromSet(labels.Set{"ome.io/pool": "online"}),
		FieldSelector: fields.OneTermEqualSelector("metadata.name", "runtime-a"),
		Limit:         99,
		Raw:           raw,
	}
	namespaced := &v1beta1.ServingRuntimeList{}
	require.NoError(t, resolver.client.List(context.Background(), namespaced, callerOptions))
	cluster := &v1beta1.ClusterServingRuntimeList{}
	require.NoError(t, resolver.client.List(
		context.Background(),
		cluster,
		ctrlclient.MatchingLabels{"ome.io/pool": "shared"},
	))

	assert.Equal(t, []string{"runtime-a", "runtime-b", "runtime-c"}, servingRuntimeNames(namespaced.Items))
	assert.Equal(t, []string{"cluster-a", "cluster-b"}, clusterServingRuntimeNames(cluster.Items))
	assert.Empty(t, namespaced.Continue)
	assert.Empty(t, cluster.Continue)
	require.Len(t, requests, 3)
	assert.Equal(t, []runtimeCandidateRequest{
		{
			kind: "ServingRuntime", namespace: "workloads",
			labelSelector: "ome.io/pool=online", fieldSelector: "metadata.name=runtime-a",
			resourceVersion: "resource-version-7", limit: 2,
			deadline: requests[0].deadline,
		},
		{
			kind: "ServingRuntime", namespace: "workloads",
			labelSelector: "ome.io/pool=online", fieldSelector: "metadata.name=runtime-a",
			resourceVersion: "resource-version-7", limit: 2, continueToken: "next-runtime-page",
			deadline: requests[1].deadline,
		},
		{
			kind: "ClusterServingRuntime", labelSelector: "ome.io/pool=shared", limit: 2,
			deadline: requests[2].deadline,
		},
	}, requests)
	for _, request := range requests {
		assert.False(t, request.deadline.IsZero(), "every candidate LIST must have a deadline")
		assert.LessOrEqual(t, time.Until(request.deadline), time.Second)
	}

	// Applying page options must not mutate options owned by the caller.
	assert.Equal(t, int64(99), callerOptions.Limit)
	assert.Zero(t, raw.Limit)
	assert.Empty(t, raw.Continue)
}

func TestBoundedRuntimeResolverCachesSelectorRetry(t *testing.T) {
	t.Parallel()

	var requests []runtimeCandidateRequest
	base := newRuntimeCandidateBaseClient(t)
	client := &runtimeCandidateListClient{Client: base}
	client.list = func(ctx context.Context, list ctrlclient.ObjectList, opts ...ctrlclient.ListOption) error {
		requests = append(requests, recordRuntimeCandidateRequest(ctx, list, opts...))
		return nil
	}
	limits := candidateLimits()
	limits.MaxItems = 10
	limits.MaxPages = 2
	resolver, err := NewBoundedRuntimeResolver(client, limits)
	require.NoError(t, err)

	_, err = resolver.selector.SelectRuntime(
		context.Background(),
		&v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "unsupported"}},
		&v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "workloads"}},
	)

	var noRuntime *runtimeselector.NoRuntimeFoundError
	require.ErrorAs(t, err, &noRuntime)
	assert.Zero(t, noRuntime.TotalRuntimes)
	require.Len(t, requests, 2, "the selector's second diagnostic fetch must use complete cached lists")
	assert.Equal(t, []string{"ServingRuntime", "ClusterServingRuntime"}, []string{
		requests[0].kind,
		requests[1].kind,
	})
}

func TestBoundedRuntimeResolverReturnsDefensiveCacheCopies(t *testing.T) {
	t.Parallel()

	calls := 0
	base := newRuntimeCandidateBaseClient(t)
	client := &runtimeCandidateListClient{Client: base}
	client.list = func(_ context.Context, list ctrlclient.ObjectList, _ ...ctrlclient.ListOption) error {
		calls++
		typed, ok := list.(*v1beta1.ServingRuntimeList)
		require.True(t, ok)
		typed.Items = []v1beta1.ServingRuntime{{ObjectMeta: metav1.ObjectMeta{
			Name: "authoritative-runtime", Namespace: "workloads",
		}}}
		return nil
	}
	resolver, err := NewBoundedRuntimeResolver(client, candidateLimits())
	require.NoError(t, err)

	first := &v1beta1.ServingRuntimeList{}
	require.NoError(t, resolver.client.List(context.Background(), first, ctrlclient.InNamespace("workloads")))
	first.Items[0].Name = "caller-mutated-runtime"
	second := &v1beta1.ServingRuntimeList{}
	require.NoError(t, resolver.client.List(context.Background(), second, ctrlclient.InNamespace("workloads")))

	assert.Equal(t, "authoritative-runtime", second.Items[0].Name)
	assert.Equal(t, 1, calls)
}

func TestBoundedRuntimeResolverDoesNotServeCacheAfterCancellation(t *testing.T) {
	t.Parallel()

	base := newRuntimeCandidateBaseClient(t)
	client := &runtimeCandidateListClient{Client: base}
	client.list = func(_ context.Context, list ctrlclient.ObjectList, _ ...ctrlclient.ListOption) error {
		typed, ok := list.(*v1beta1.ServingRuntimeList)
		require.True(t, ok)
		typed.Items = []v1beta1.ServingRuntime{{ObjectMeta: metav1.ObjectMeta{Name: "runtime-a"}}}
		return nil
	}
	resolver, err := NewBoundedRuntimeResolver(client, candidateLimits())
	require.NoError(t, err)
	require.NoError(t, resolver.client.List(context.Background(), &v1beta1.ServingRuntimeList{}))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	err = resolver.client.List(canceled, &v1beta1.ServingRuntimeList{})

	require.ErrorIs(t, err, context.Canceled)
}

func TestBoundedRuntimeResolverRejectsRuntimeSnapshotChangeWithoutReflectingMetadata(t *testing.T) {
	t.Parallel()

	const (
		secretUID = "runtime-uid-with-user-secret"
		secretRV  = "runtime-rv-with-user-secret"
		secretEnv = "runtime-env-with-user-secret"
	)
	base := newRuntimeCandidateBaseClient(t)
	client := &runtimeCandidateListClient{Client: base}
	client.list = func(_ context.Context, list ctrlclient.ObjectList, _ ...ctrlclient.ListOption) error {
		typed, ok := list.(*v1beta1.ServingRuntimeList)
		require.True(t, ok)
		typed.Items = []v1beta1.ServingRuntime{{
			ObjectMeta: metav1.ObjectMeta{
				Name: "runtime", Namespace: "workloads", UID: "runtime-uid", ResourceVersion: "1", Generation: 1,
			},
			Spec: v1beta1.ServingRuntimeSpec{},
		}}
		return nil
	}
	client.get = func(_ context.Context, _ ctrlclient.ObjectKey, object ctrlclient.Object, _ ...ctrlclient.GetOption) error {
		typed, ok := object.(*v1beta1.ServingRuntime)
		require.True(t, ok)
		typed.ObjectMeta = metav1.ObjectMeta{
			Name: "runtime", Namespace: "workloads", UID: secretUID, ResourceVersion: secretRV, Generation: 2,
			Annotations: map[string]string{"ome.io/runtime-inherit-from": secretEnv},
		}
		return nil
	}
	resolver, err := NewBoundedRuntimeResolver(client, candidateLimits())
	require.NoError(t, err)
	require.NoError(t, resolver.client.List(
		context.Background(), &v1beta1.ServingRuntimeList{}, ctrlclient.InNamespace("workloads"),
	))

	err = resolver.client.Get(
		context.Background(), ctrlclient.ObjectKey{Namespace: "workloads", Name: "runtime"}, &v1beta1.ServingRuntime{},
	)

	require.ErrorIs(t, err, ErrRuntimeSnapshotChanged)
	for _, rendered := range []string{err.Error(), fmt.Sprintf("%+v", err), fmt.Sprintf("%#v", err)} {
		assert.NotContains(t, rendered, secretUID)
		assert.NotContains(t, rendered, secretRV)
		assert.NotContains(t, rendered, secretEnv)
	}
}

func TestRuntimeResolverBindsExactRuntimeSnapshotIdentity(t *testing.T) {
	t.Parallel()

	runtimeObject := &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{
		Name: "runtime", Namespace: "workloads", UID: "runtime-uid", ResourceVersion: "17", Generation: 4,
	}}
	client := newRuntimeCandidateBaseClient(t, runtimeObject)
	resolver := NewRuntimeResolver(client)
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "workloads"},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: "runtime", Kind: ptr.To(runtimeselector.KindServingRuntime)},
			Engine:  &v1beta1.EngineSpec{},
		},
	}

	resolved, err := resolver.ResolveLive(context.Background(), isvc)

	require.NoError(t, err)
	assert.True(t, resolved.Runtime.IdentityObserved)
	assert.Equal(t, "runtime-uid", resolved.Runtime.UID)
	assert.Equal(t, int64(4), resolved.Runtime.Generation)
}

func TestRuntimeResolverSnapshotBindingIsScopedToOneResolution(t *testing.T) {
	t.Parallel()

	phase := int64(1)
	base := newRuntimeCandidateBaseClient(t)
	client := &runtimeCandidateListClient{Client: base}
	client.get = func(_ context.Context, key ctrlclient.ObjectKey, object ctrlclient.Object, _ ...ctrlclient.GetOption) error {
		typed, ok := object.(*v1beta1.ServingRuntime)
		require.True(t, ok)
		typed.ObjectMeta = metav1.ObjectMeta{
			Name: key.Name, Namespace: key.Namespace, UID: "runtime-uid",
			ResourceVersion: fmt.Sprintf("%d", phase), Generation: phase,
		}
		typed.Spec = v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MaxReplicas: int(phase)},
		}}
		return nil
	}
	resolver := NewRuntimeResolver(client)
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "workloads"},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: "runtime", Kind: ptr.To(runtimeselector.KindServingRuntime)},
			Engine:  &v1beta1.EngineSpec{},
		},
	}

	first, err := resolver.ResolveLive(context.Background(), isvc)
	require.NoError(t, err)
	assert.Equal(t, int64(1), first.Runtime.Generation)
	phase = 2
	second, err := resolver.ResolveLive(context.Background(), isvc)

	require.NoError(t, err)
	assert.Equal(t, int64(2), second.Runtime.Generation)
}

func TestBoundedRuntimeResolverResetsCandidateCacheForEachResolution(t *testing.T) {
	t.Parallel()

	phase := 1
	listCalls := 0
	runtimeObject := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "runtime", Namespace: "workloads", UID: "runtime-uid", ResourceVersion: "17",
		},
		Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}},
	}
	base := newRuntimeCandidateBaseClient(t, runtimeObject)
	client := &runtimeCandidateListClient{Client: base}
	client.list = func(_ context.Context, list ctrlclient.ObjectList, _ ...ctrlclient.ListOption) error {
		listCalls++
		typed, ok := list.(*v1beta1.ServingRuntimeList)
		require.True(t, ok)
		typed.Items = []v1beta1.ServingRuntime{{ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("candidate-%d", phase), Namespace: "workloads",
		}}}
		return nil
	}
	resolver, err := NewBoundedRuntimeResolver(client, candidateLimits())
	require.NoError(t, err)

	first := &v1beta1.ServingRuntimeList{}
	require.NoError(t, resolver.client.List(context.Background(), first, ctrlclient.InNamespace("workloads")))
	assert.Equal(t, []string{"candidate-1"}, servingRuntimeNames(first.Items))

	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "workloads"},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: "runtime", Kind: ptr.To(runtimeselector.KindServingRuntime)},
			Engine:  &v1beta1.EngineSpec{},
		},
	}
	phase = 2
	_, err = resolver.ResolveLive(context.Background(), isvc)
	require.NoError(t, err)
	second := &v1beta1.ServingRuntimeList{}
	require.NoError(t, resolver.client.List(context.Background(), second, ctrlclient.InNamespace("workloads")))

	assert.Equal(t, []string{"candidate-2"}, servingRuntimeNames(second.Items))
	assert.Equal(t, 2, listCalls)
}

func TestBoundedRuntimeResolverFailsClosedAtContinuation(t *testing.T) {
	t.Parallel()

	const (
		secretRuntime = "runtime-with-user-secret"
		secretToken   = "continue-token-with-user-secret"
	)
	base := newRuntimeCandidateBaseClient(t)
	client := &runtimeCandidateListClient{Client: base}
	client.list = func(_ context.Context, list ctrlclient.ObjectList, _ ...ctrlclient.ListOption) error {
		typed, ok := list.(*v1beta1.ServingRuntimeList)
		require.True(t, ok)
		typed.Items = []v1beta1.ServingRuntime{{ObjectMeta: metav1.ObjectMeta{Name: secretRuntime, Namespace: "workloads"}}}
		typed.Continue = secretToken
		return nil
	}
	limits := candidateLimits()
	limits.PageSize = 1
	limits.MaxPages = 1
	resolver, err := NewBoundedRuntimeResolver(client, limits)
	require.NoError(t, err)

	result := &v1beta1.ServingRuntimeList{}
	err = resolver.client.List(context.Background(), result, ctrlclient.InNamespace("workloads"))

	var truncated *RuntimeSelectionTruncated
	require.ErrorAs(t, err, &truncated)
	assert.Empty(t, result.Items, "partial candidates must never reach runtime selection")
	for _, rendered := range []string{err.Error(), fmt.Sprintf("%+v", err), fmt.Sprintf("%#v", truncated)} {
		assert.NotContains(t, rendered, secretRuntime)
		assert.NotContains(t, rendered, secretToken)
	}
}

func TestBoundedRuntimeResolverSharesLimitsAcrossCandidateKinds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		limits    paging.Limits
		serving   []v1beta1.ServingRuntime
		wantFirst []string
		wantCalls int
	}{
		{
			name: "items",
			limits: paging.Limits{
				PageSize: 1, MaxItems: 1, MaxPages: 2, RequestTimeout: time.Second,
			},
			serving:   []v1beta1.ServingRuntime{{ObjectMeta: metav1.ObjectMeta{Name: "only-item"}}},
			wantFirst: []string{"only-item"},
			wantCalls: 1,
		},
		{
			name: "pages",
			limits: paging.Limits{
				PageSize: 1, MaxItems: 2, MaxPages: 1, RequestTimeout: time.Second,
			},
			wantFirst: []string{},
			wantCalls: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			base := newRuntimeCandidateBaseClient(t)
			client := &runtimeCandidateListClient{Client: base}
			client.list = func(_ context.Context, list ctrlclient.ObjectList, _ ...ctrlclient.ListOption) error {
				calls++
				typed, ok := list.(*v1beta1.ServingRuntimeList)
				require.True(t, ok, "cluster LIST must be rejected before reaching the API client")
				typed.Items = append([]v1beta1.ServingRuntime{}, test.serving...)
				return nil
			}
			resolver, err := NewBoundedRuntimeResolver(client, test.limits)
			require.NoError(t, err)

			first := &v1beta1.ServingRuntimeList{}
			require.NoError(t, resolver.client.List(context.Background(), first))
			assert.Equal(t, test.wantFirst, servingRuntimeNames(first.Items))
			second := &v1beta1.ClusterServingRuntimeList{}
			err = resolver.client.List(context.Background(), second)

			var truncated *RuntimeSelectionTruncated
			require.ErrorAs(t, err, &truncated)
			assert.Empty(t, second.Items)
			assert.Equal(t, test.wantCalls, calls)
		})
	}
}

func TestBoundedRuntimeResolverRejectsServerItemOverrun(t *testing.T) {
	tests := []struct {
		name     string
		result   ctrlclient.ObjectList
		populate func(ctrlclient.ObjectList)
	}{
		{
			name: "namespaced runtimes", result: &v1beta1.ServingRuntimeList{},
			populate: func(list ctrlclient.ObjectList) {
				list.(*v1beta1.ServingRuntimeList).Items = []v1beta1.ServingRuntime{
					{ObjectMeta: metav1.ObjectMeta{Name: "runtime-a"}},
					{ObjectMeta: metav1.ObjectMeta{Name: "runtime-b"}},
				}
			},
		},
		{
			name: "cluster runtimes", result: &v1beta1.ClusterServingRuntimeList{},
			populate: func(list ctrlclient.ObjectList) {
				list.(*v1beta1.ClusterServingRuntimeList).Items = []v1beta1.ClusterServingRuntime{
					{ObjectMeta: metav1.ObjectMeta{Name: "runtime-a"}},
					{ObjectMeta: metav1.ObjectMeta{Name: "runtime-b"}},
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			base := newRuntimeCandidateBaseClient(t)
			client := &runtimeCandidateListClient{Client: base}
			client.list = func(_ context.Context, list ctrlclient.ObjectList, _ ...ctrlclient.ListOption) error {
				tt.populate(list)
				return nil
			}
			limits := candidateLimits()
			limits.PageSize = 1
			limits.MaxItems = 1
			resolver, err := NewBoundedRuntimeResolver(client, limits)
			require.NoError(t, err)

			err = resolver.client.List(context.Background(), tt.result)

			var truncated *RuntimeSelectionTruncated
			require.ErrorAs(t, err, &truncated)
			bounded, ok := resolver.client.(*boundedRuntimeCandidateClient)
			require.True(t, ok)
			bounded.snapshots.mu.Lock()
			retained := len(bounded.snapshots.snapshots)
			bounded.snapshots.mu.Unlock()
			assert.Zero(t, retained, "a hostile overrun response must not be fingerprinted or retained")
		})
	}
}

func TestBoundedRuntimeResolverRejectsOutOfScopeCandidatesWithoutDisclosure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		list      ctrlclient.ObjectList
		options   []ctrlclient.ListOption
		populate  func(ctrlclient.ObjectList)
		itemCount func(ctrlclient.ObjectList) int
	}{
		{
			name: "namespaced runtime", list: &v1beta1.ServingRuntimeList{},
			options: []ctrlclient.ListOption{ctrlclient.InNamespace("workloads")},
			populate: func(list ctrlclient.ObjectList) {
				list.(*v1beta1.ServingRuntimeList).Items = []v1beta1.ServingRuntime{{ObjectMeta: metav1.ObjectMeta{
					Name: "SECRET_RUNTIME", Namespace: "SECRET_NAMESPACE",
				}}}
			},
			itemCount: func(list ctrlclient.ObjectList) int { return len(list.(*v1beta1.ServingRuntimeList).Items) },
		},
		{
			name: "cluster runtime", list: &v1beta1.ClusterServingRuntimeList{},
			populate: func(list ctrlclient.ObjectList) {
				list.(*v1beta1.ClusterServingRuntimeList).Items = []v1beta1.ClusterServingRuntime{{ObjectMeta: metav1.ObjectMeta{
					Name: "SECRET_CLUSTER_RUNTIME", Namespace: "SECRET_NAMESPACE",
				}}}
			},
			itemCount: func(list ctrlclient.ObjectList) int { return len(list.(*v1beta1.ClusterServingRuntimeList).Items) },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			base := newRuntimeCandidateBaseClient(t)
			client := &runtimeCandidateListClient{Client: base}
			client.list = func(_ context.Context, list ctrlclient.ObjectList, _ ...ctrlclient.ListOption) error {
				calls++
				test.populate(list)
				return nil
			}
			resolver, err := NewBoundedRuntimeResolver(client, candidateLimits())
			require.NoError(t, err)

			err = resolver.client.List(context.Background(), test.list, test.options...)

			require.ErrorIs(t, err, ErrRuntimeObjectIdentityMismatch)
			assert.Zero(t, test.itemCount(test.list), "out-of-scope candidates must never reach selection")
			assert.NotContains(t, err.Error(), "SECRET_RUNTIME")
			assert.NotContains(t, err.Error(), "SECRET_CLUSTER_RUNTIME")
			assert.NotContains(t, err.Error(), "SECRET_NAMESPACE")
			assert.Equal(t, 1, calls)
		})
	}
}

func TestBoundedRuntimeResolverAppliesPerRequestTimeout(t *testing.T) {
	t.Parallel()

	base := newRuntimeCandidateBaseClient(t)
	client := &runtimeCandidateListClient{Client: base}
	client.list = func(ctx context.Context, _ ctrlclient.ObjectList, _ ...ctrlclient.ListOption) error {
		<-ctx.Done()
		return ctx.Err()
	}
	limits := candidateLimits()
	limits.RequestTimeout = 10 * time.Millisecond
	resolver, err := NewBoundedRuntimeResolver(client, limits)
	require.NoError(t, err)

	err = resolver.client.List(context.Background(), &v1beta1.ServingRuntimeList{})

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestBoundedRuntimeResolverDelegatesNonCandidateReads(t *testing.T) {
	t.Parallel()

	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model-a", Namespace: "workloads"}}
	base := newRuntimeCandidateBaseClient(t, model)
	resolver, err := NewBoundedRuntimeResolver(base, paging.Limits{
		PageSize: 1, MaxItems: 1, MaxPages: 1, RequestTimeout: time.Second,
	})
	require.NoError(t, err)

	got := &v1beta1.BaseModel{}
	require.NoError(t, resolver.client.Get(
		context.Background(),
		ctrlclient.ObjectKey{Name: model.Name, Namespace: model.Namespace},
		got,
	))
	assert.Equal(t, model.Name, got.Name)
	models := &v1beta1.BaseModelList{}
	require.NoError(t, resolver.client.List(context.Background(), models, ctrlclient.InNamespace("workloads")))
	assert.Equal(t, []string{"model-a"}, baseModelNames(models.Items))

	// A delegated list must not consume the candidate page budget.
	require.NoError(t, resolver.client.List(context.Background(), &v1beta1.ServingRuntimeList{}))
}

func TestBoundedRuntimeResolverRejectsMismatchedGetIdentityWithoutDisclosure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		key      ctrlclient.ObjectKey
		object   ctrlclient.Object
		returned ctrlclient.ObjectKey
	}{
		{
			name: "BaseModel name", key: ctrlclient.ObjectKey{Name: "requested-model", Namespace: "workloads"},
			object: &v1beta1.BaseModel{}, returned: ctrlclient.ObjectKey{Name: "SECRET_RETURNED_MODEL", Namespace: "workloads"},
		},
		{
			name: "ClusterBaseModel name", key: ctrlclient.ObjectKey{Name: "requested-model"},
			object: &v1beta1.ClusterBaseModel{}, returned: ctrlclient.ObjectKey{Name: "SECRET_RETURNED_CLUSTER_MODEL"},
		},
		{
			name: "ServingRuntime namespace", key: ctrlclient.ObjectKey{Name: "requested-runtime", Namespace: "workloads"},
			object: &v1beta1.ServingRuntime{}, returned: ctrlclient.ObjectKey{Name: "requested-runtime", Namespace: "SECRET_RETURNED_NAMESPACE"},
		},
		{
			name: "ClusterServingRuntime name", key: ctrlclient.ObjectKey{Name: "requested-runtime"},
			object: &v1beta1.ClusterServingRuntime{}, returned: ctrlclient.ObjectKey{Name: "SECRET_RETURNED_CLUSTER_RUNTIME"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			base := newRuntimeCandidateBaseClient(t)
			client := &runtimeCandidateListClient{Client: base}
			client.get = func(_ context.Context, _ ctrlclient.ObjectKey, object ctrlclient.Object, _ ...ctrlclient.GetOption) error {
				object.SetName(test.returned.Name)
				object.SetNamespace(test.returned.Namespace)
				return nil
			}
			resolver, err := NewBoundedRuntimeResolver(client, candidateLimits())
			require.NoError(t, err)

			err = resolver.client.Get(context.Background(), test.key, test.object)

			require.ErrorIs(t, err, ErrRuntimeObjectIdentityMismatch)
			for _, secret := range []string{test.key.Name, test.key.Namespace, test.returned.Name, test.returned.Namespace} {
				if secret != "" {
					assert.NotContains(t, err.Error(), secret)
				}
			}
		})
	}
}

func TestNewBoundedRuntimeResolverRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	valid := candidateLimits()
	tests := []struct {
		name   string
		client ctrlclient.Client
		limits paging.Limits
	}{
		{name: "nil client", limits: valid},
		{name: "page size", client: newRuntimeCandidateBaseClient(t), limits: paging.Limits{
			MaxItems: 1, MaxPages: 1, RequestTimeout: time.Second,
		}},
		{name: "item limit", client: newRuntimeCandidateBaseClient(t), limits: paging.Limits{
			PageSize: 1, MaxPages: 1, RequestTimeout: time.Second,
		}},
		{name: "page limit", client: newRuntimeCandidateBaseClient(t), limits: paging.Limits{
			PageSize: 1, MaxItems: 1, RequestTimeout: time.Second,
		}},
		{name: "request timeout", client: newRuntimeCandidateBaseClient(t), limits: paging.Limits{
			PageSize: 1, MaxItems: 1, MaxPages: 1,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			resolver, err := NewBoundedRuntimeResolver(test.client, test.limits)

			require.Error(t, err)
			assert.Nil(t, resolver)
		})
	}
}

func servingRuntimeNames(items []v1beta1.ServingRuntime) []string {
	names := make([]string, 0, len(items))
	for i := range items {
		names = append(names, items[i].Name)
	}
	return names
}

func clusterServingRuntimeNames(items []v1beta1.ClusterServingRuntime) []string {
	names := make([]string, 0, len(items))
	for i := range items {
		names = append(names, items[i].Name)
	}
	return names
}

func baseModelNames(items []v1beta1.BaseModel) []string {
	names := make([]string, 0, len(items))
	for i := range items {
		names = append(names, items[i].Name)
	}
	return names
}
