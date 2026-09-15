package mutate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubefake "k8s.io/client-go/kubernetes/fake"
	appsclient "k8s.io/client-go/kubernetes/typed/apps/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimerevision"
)

type heldFallbackClient struct {
	ctrlclient.Client
	mu           sync.Mutex
	missingKey   ctrlclient.ObjectKey
	forbidden    bool
	clusterReads int
}

func (c *heldFallbackClient) Get(ctx context.Context, key ctrlclient.ObjectKey, value ctrlclient.Object, opts ...ctrlclient.GetOption) error {
	c.mu.Lock()
	if key.Namespace == "" {
		c.clusterReads++
	}
	c.mu.Unlock()
	if c.forbidden && key == c.missingKey {
		return apierrors.NewForbidden(schema.GroupResource{Group: "ome.io", Resource: "source"}, "PRIVATE_API_NAME", errors.New("PRIVATE_API_PROSE"))
	}
	return c.Client.Get(ctx, key, value, opts...)
}

func TestHeldReleaseRuntimePreservesEstablishedNotFoundFallback(t *testing.T) {
	for _, scenario := range []string{"cluster model", "cluster inheritance"} {
		for _, forbidden := range []bool{false, true} {
			t.Run(fmt.Sprint(scenario, "/forbidden=", forbidden), func(t *testing.T) {
				v, _ := nativeTarget(t)
				rt := &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: "prod", UID: "uid-runtime", ResourceVersion: "99", Generation: 1}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "busybox"}}}}}
				objects := []ctrlclient.Object{rt}
				missing := ctrlclient.ObjectKey{Namespace: "prod"}
				if scenario == "cluster model" {
					v.Spec.Model = &v1beta1.ModelRef{Name: "llama"}
					model := &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "llama", UID: "uid-model", ResourceVersion: "9", Generation: 1}}
					model.Spec.ModelFormat.Name = "safetensors"
					objects = append(objects, model)
					missing.Name = "llama"
				} else {
					rt.Annotations = map[string]string{constants.RuntimeInheritFromAnnotationKey: "base"}
					objects = append(objects, &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "base", UID: "uid-base", ResourceVersion: "10", Generation: 1}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}}})
					missing.Name = "base"
				}
				scheme := runtime.NewScheme()
				require.NoError(t, v1beta1.AddToScheme(scheme))
				base := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
				apps := kubefake.NewClientset().AppsV1()
				// The established resolver baseline proves the fixture's fallback;
				// the held admission wrapper must preserve exactly that behavior.
				for _, wrapped := range []bool{false, true} {
					trace := &heldFallbackClient{Client: base, missingKey: missing, forbidden: forbidden}
					var resolver *effective.RuntimePinResolver
					var err error
					if wrapped {
						resolver, _, err = NewHeldReleaseRuntimeResolver(apps, trace, "ome", time.Second)
					} else {
						resolver, err = effective.NewRuntimePinResolver(apps, effective.NewRuntimeResolver(trace), "ome", paging.Limits{PageSize: 16, MaxItems: 32, MaxPages: 2, RequestTimeout: time.Second})
					}
					require.NoError(t, err)
					state, err := resolver.Resolve(context.Background(), v, effective.RuntimeResolveOptions{})
					if err == nil {
						_, err = RequireNativeRuntime(v, state)
					}
					if forbidden {
						require.Error(t, err)
						require.Zero(t, trace.clusterReads)
						if wrapped {
							require.NotContains(t, err.Error(), "PRIVATE")
						}
					} else {
						require.NoError(t, err, "wrapped=%v", wrapped)
						require.Positive(t, trace.clusterReads)
						if wrapped {
							evidence, e := CollectHeldReleaseEvidence(context.Background(), omefake.NewSimpleClientset(heldReplica(v)).OmeV1beta1(), v, "engine", testClock)
							require.NoError(t, e)
							_, e = PrepareHeldRelease(v, state, evidence, "engine", "aaaaaaaa", testClock)
							require.NoError(t, e)
						}
					}
				}
			})
		}
	}
}

func TestHeldReleaseRuntimeRejectsConflictingPartialGVK(t *testing.T) {
	for _, gvk := range []schema.GroupVersionKind{{Group: "other.io"}, {Kind: "Other"}, {Group: "ome.io", Version: "v2", Kind: "ServingRuntime"}} {
		value := &v1beta1.ServingRuntime{}
		value.SetGroupVersionKind(gvk)
		require.Error(t, validateHeldRuntimeGVK(value))
	}
}

type heldRevisionReply struct {
	appsclient.ControllerRevisionInterface
	value *appsv1.ControllerRevision
	list  *appsv1.ControllerRevisionList
	err   error
}

func (r heldRevisionReply) Get(context.Context, string, metav1.GetOptions) (*appsv1.ControllerRevision, error) {
	return r.value, r.err
}
func (r heldRevisionReply) List(context.Context, metav1.ListOptions) (*appsv1.ControllerRevisionList, error) {
	return r.list, r.err
}

type heldRuntimeListReply struct {
	ctrlclient.Client
	list   *v1beta1.ServingRuntimeList
	err    error
	cancel context.CancelFunc
}

func (r heldRuntimeListReply) List(_ context.Context, value ctrlclient.ObjectList, _ ...ctrlclient.ListOption) error {
	if r.cancel != nil {
		r.cancel()
	}
	if r.err != nil {
		return r.err
	}
	*value.(*v1beta1.ServingRuntimeList) = *r.list.DeepCopy()
	return nil
}

func TestHeldReleaseRuntimeListRejectsOversizedPageAndWrongGVK(t *testing.T) {
	for _, scenario := range []string{"oversized page", "wrong list GVK", "wrong namespace"} {
		value := v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: "prod", UID: "uid-runtime", ResourceVersion: "99", Generation: 1}}
		list := &v1beta1.ServingRuntimeList{Items: []v1beta1.ServingRuntime{value}}
		switch scenario {
		case "oversized page":
			for n := 0; n < 16; n++ {
				copy := *value.DeepCopy()
				copy.Name = fmt.Sprint("runtime-", n)
				list.Items = append(list.Items, copy)
			}
		case "wrong list GVK":
			list.Kind = "Other"
		case "wrong namespace":
			list.Items[0].Namespace = "other"
		}
		c := heldRuntimeClient{Client: heldRuntimeListReply{list: list}, binding: &HeldRuntimeBinding{objects: map[string]runtime.Object{}}}
		require.Error(t, c.List(context.Background(), &v1beta1.ServingRuntimeList{}, ctrlclient.InNamespace("prod")))
	}
}

func TestHeldReleaseRuntimeAndRevisionListsWholeAdmission(t *testing.T) {
	for _, scenario := range []string{"valid", "private error", "canceled", "oversized bytes", "bad item"} {
		t.Run("runtime/"+scenario, func(t *testing.T) {
			value := v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: "prod", UID: "uid-runtime", ResourceVersion: "99", Generation: 1}}
			list := &v1beta1.ServingRuntimeList{Items: []v1beta1.ServingRuntime{value}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reply := heldRuntimeListReply{list: list}
			switch scenario {
			case "private error":
				reply.err = errors.New("PRIVATE_LIST_ERROR")
			case "canceled":
				reply.cancel = cancel
			case "oversized bytes":
				list.Items[0].Annotations = map[string]string{"private": strings.Repeat("PRIVATE", 160000)}
			case "bad item":
				list.Items[0].Kind = "Other"
			}
			binding := &HeldRuntimeBinding{objects: map[string]runtime.Object{}}
			c := heldRuntimeClient{Client: reply, binding: binding}
			err := c.List(ctx, &v1beta1.ServingRuntimeList{}, ctrlclient.InNamespace("prod"))
			if scenario == "valid" {
				require.NoError(t, err)
				require.Len(t, binding.objects, 1)
			} else {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "PRIVATE")
				require.Empty(t, binding.objects)
			}
		})
	}
	for _, scenario := range []string{"valid", "nil list", "private error", "oversized page", "oversized bytes", "bad item", "bad list"} {
		t.Run("revision/"+scenario, func(t *testing.T) {
			value := appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "revision", Namespace: "ome", UID: "uid-revision", ResourceVersion: "91"}}
			reply := heldRevisionReply{list: &appsv1.ControllerRevisionList{Items: []appsv1.ControllerRevision{value}}}
			switch scenario {
			case "nil list":
				reply.list = nil
			case "private error":
				reply.err = errors.New("PRIVATE_LIST_ERROR")
			case "oversized page":
				reply.list.Items = make([]appsv1.ControllerRevision, 17)
			case "oversized bytes":
				reply.list.Items[0].Data.Raw = []byte(strings.Repeat("PRIVATE", 160000))
			case "bad item":
				reply.list.Items[0].Kind = "Other"
			case "bad list":
				reply.list.Kind = "Other"
			}
			binding := &HeldRuntimeBinding{objects: map[string]runtime.Object{}}
			c := heldRevisionClient{ControllerRevisionInterface: reply, binding: binding}
			_, err := c.List(context.Background(), metav1.ListOptions{})
			if scenario == "valid" {
				require.NoError(t, err)
				require.Len(t, binding.objects, 1)
			} else {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "PRIVATE")
				require.Empty(t, binding.objects)
			}
		})
	}
}

func TestHeldReleaseClosedRuntimeTypesAndScopeIdentity(t *testing.T) {
	for _, value := range []runtime.Object{&v1beta1.ClusterServingRuntime{}, &v1beta1.BaseModel{}, &v1beta1.ClusterBaseModel{}, &v1beta1.ServingRuntimeList{}, &v1beta1.ClusterServingRuntimeList{}, &v1beta1.BaseModelList{}, &v1beta1.ClusterBaseModelList{}, &appsv1.ControllerRevisionList{}} {
		require.NoError(t, validateHeldRuntimeGVK(value))
	}
	require.Error(t, validateHeldRuntimeGVK(&corev1.Secret{}))
	value := &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime", UID: "uid-runtime", ResourceVersion: "99", Generation: 1}}
	require.NoError(t, validateHeldRuntimeIdentity(value))
	value.Namespace = "prod"
	require.Error(t, validateHeldRuntimeIdentity(value))
	_, _, err := NewHeldReleaseRuntimeResolver(nil, nil, "ome")
	require.Error(t, err)
	b := &HeldRuntimeBinding{objects: map[string]runtime.Object{}}
	require.True(t, b.Matches(b))
	require.Equal(t, "<HeldRuntimeBinding redacted>", fmt.Sprint(b))
	require.Equal(t, "<HeldReleaseEvidence redacted>", fmt.Sprintf("%#v", HeldReleaseEvidence{}))
}

func TestHeldReleaseRevisionWrapperRejectsWrongGVK(t *testing.T) {
	value := &appsv1.ControllerRevision{TypeMeta: metav1.TypeMeta{APIVersion: "other.io/v1", Kind: "Other"}, ObjectMeta: metav1.ObjectMeta{Name: "revision", Namespace: "ome"}}
	c := heldRevisionClient{ControllerRevisionInterface: heldRevisionReply{value: value}, binding: &HeldRuntimeBinding{objects: map[string]runtime.Object{}}}
	_, err := c.Get(context.Background(), "revision", metav1.GetOptions{})
	require.Error(t, err)
}

func TestHeldReleaseRuntimeBindingBoundsAndNoPrivateFingerprintOutput(t *testing.T) {
	newBinding := func() *HeldRuntimeBinding { return &HeldRuntimeBinding{objects: map[string]runtime.Object{}} }
	value := &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: "prod"}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "busybox", Env: []corev1.EnvVar{{Name: "TOKEN", Value: "PRIVATE_RUNTIME_TOKEN"}}}}}}}
	a, b := newBinding(), newBinding()
	require.NoError(t, a.admit(value))
	require.NoError(t, b.admit(value))
	require.True(t, a.Matches(b))
	require.False(t, a.Matches(nil))
	require.Equal(t, "<HeldRuntimeBinding redacted>", fmt.Sprintf("%#v", a))
	value.Spec.EngineConfig.Runner.Env[0].Value = "CHANGED_PRIVATE_TOKEN"
	require.ErrorIs(t, a.admit(value), ErrStale)
	require.True(t, a.Matches(b))
	large := value.DeepCopy()
	large.Spec.EngineConfig.Runner.Env[0].Value = strings.Repeat("PRIVATE", 160000)
	require.ErrorIs(t, newBinding().admit(large), ErrBounds)
	a = newBinding()
	for n := 0; n < 96; n++ {
		require.NoError(t, a.admit(value))
	}
	require.ErrorIs(t, a.admit(value), ErrBounds)
	a = newBinding()
	for n := 0; n < 64; n++ {
		copy := value.DeepCopy()
		copy.Name = fmt.Sprint("runtime-", n)
		require.NoError(t, a.admit(copy))
	}
	copy := value.DeepCopy()
	copy.Name = "runtime-64"
	require.ErrorIs(t, a.admit(copy), ErrBounds)
	a = newBinding()
	require.ErrorIs(t, a.admit(nil), ErrBounds)
}

type heldRuntimeReply struct {
	ctrlclient.Client
	value  *v1beta1.ServingRuntime
	err    error
	cancel context.CancelFunc
}

func (r heldRuntimeReply) Get(_ context.Context, _ ctrlclient.ObjectKey, value ctrlclient.Object, _ ...ctrlclient.GetOption) error {
	if r.cancel != nil {
		r.cancel()
	}
	if r.err != nil {
		return r.err
	}
	*value.(*v1beta1.ServingRuntime) = *r.value.DeepCopy()
	return nil
}

func TestHeldReleaseRuntimeWrapperChecksOriginalBeforeResolverCopies(t *testing.T) {
	for _, scenario := range []string{"valid", "wrong identity", "missing UID", "unsafe RV", "zero generation", "deleting", "private error", "ignoring cancellation", "oversized"} {
		t.Run(scenario, func(t *testing.T) {
			value := &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: "prod", UID: "uid-runtime", ResourceVersion: "99", Generation: 1}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reply := heldRuntimeReply{value: value}
			switch scenario {
			case "wrong identity":
				value.Name = "other"
			case "missing UID":
				value.UID = ""
			case "unsafe RV":
				value.ResourceVersion = "PRIVATE\nRV"
			case "zero generation":
				value.Generation = 0
			case "deleting":
				stamp := metav1.NewTime(testNow)
				value.DeletionTimestamp = &stamp
			case "private error":
				reply.err = errors.New("PRIVATE_GET_ERROR")
			case "ignoring cancellation":
				reply.cancel = cancel
			case "oversized":
				value.Annotations = map[string]string{"private": strings.Repeat("PRIVATE", 160000)}
			}
			binding := &HeldRuntimeBinding{objects: map[string]runtime.Object{}}
			c := heldRuntimeClient{Client: reply, binding: binding}
			err := c.Get(ctx, ctrlclient.ObjectKey{Namespace: "prod", Name: "simple"}, &v1beta1.ServingRuntime{})
			if scenario == "valid" {
				require.NoError(t, err)
				require.Len(t, binding.objects, 1)
			} else {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "PRIVATE")
				require.Empty(t, binding.objects)
			}
		})
	}
}

func TestHeldReleaseLiveAndConsistentPinnedNativeRuntime(t *testing.T) {
	for _, scenario := range []string{"live", "pinned", "inconsistent pinned", "oversized pinned"} {
		t.Run(scenario, func(t *testing.T) {
			v, _ := nativeTarget(t)
			spec := v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "busybox", Env: []corev1.EnvVar{{Name: "TOKEN", Value: "PRIVATE_PINNED_TOKEN"}}}}}}
			_, short, err := runtimerevision.Hash(&spec)
			require.NoError(t, err)
			name := runtimerevision.Name(runtimerevision.KindServingRuntime, "prod", "simple", short)
			body, err := json.Marshal(&spec)
			require.NoError(t, err)
			revision := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ome", UID: "uid-revision", ResourceVersion: "91", Labels: map[string]string{constants.RuntimeRevisionOfLabelKey: "simple", constants.RuntimeRevisionOfKindLabelKey: "ServingRuntime", constants.RuntimeRevisionOfNamespaceLabelKey: "prod", constants.RuntimeRevisionHashLabelKey: short}, Annotations: map[string]string{constants.RuntimeRevisionCreatedByKey: constants.RuntimeRevisionCreatedByOMEValue}}, Revision: 1, Data: runtime.RawExtension{Raw: body}}
			if scenario != "live" {
				auto := false
				v.Spec.Runtime.AutoSync = &auto
				v.Status.PinnedRevisionName = name
			}
			if scenario == "inconsistent pinned" {
				revision.Annotations = nil
			}
			if scenario == "oversized pinned" {
				revision.Data.Raw = []byte(strings.Repeat("PRIVATE", 160000))
			}
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			rt := &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: "prod", UID: "uid-runtime", ResourceVersion: "99", Generation: 1}, Spec: spec}
			resolver, binding, err := NewHeldReleaseRuntimeResolver(kubefake.NewClientset(revision).AppsV1(), clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(rt).Build(), "ome", time.Second)
			require.NoError(t, err)
			state, err := resolver.Resolve(context.Background(), v, effective.RuntimeResolveOptions{})
			if err == nil {
				_, err = RequireNativeRuntime(v, state)
			}
			if scenario == "live" || scenario == "pinned" {
				require.NoError(t, err)
				evidence, e := CollectHeldReleaseEvidence(context.Background(), omefake.NewSimpleClientset(heldReplica(v)).OmeV1beta1(), v, "engine", testClock)
				require.NoError(t, e)
				_, e = PrepareHeldRelease(v, state, evidence, "engine", "aaaaaaaa", testClock)
				require.NoError(t, e)
				require.NotEmpty(t, binding.objects)
			} else {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "PRIVATE")
			}
		})
	}
}
