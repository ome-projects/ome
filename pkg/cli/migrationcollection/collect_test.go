package migrationcollection

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ktesting "k8s.io/client-go/testing"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

var collectionTestLimits = paging.Limits{
	PageSize: 2, MaxItems: 4, MaxPages: 3, RequestTimeout: time.Second,
}

func TestCollectGetsExactParentAndPagesExactLabeledReplicas(t *testing.T) {
	t.Parallel()

	parent := collectionISVC("chat", "prod", "uid-chat")
	client := omefake.NewSimpleClientset(parent)
	client.PrependReactor("list", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
		options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		switch options.Continue {
		case "":
			return true, &omev1beta1.InferenceReplicaList{
				ListMeta: metav1.ListMeta{Continue: "next"},
				Items:    []omev1beta1.InferenceReplica{collectionIR("chat-engine", "prod", "ir-engine")},
			}, nil
		case "next":
			return true, &omev1beta1.InferenceReplicaList{
				Items: []omev1beta1.InferenceReplica{collectionIR("chat-router", "prod", "ir-router")},
			}, nil
		default:
			return true, nil, errors.New("unexpected continue token")
		}
	})

	got, err := Collect(context.Background(), client.OmeV1beta1(), "prod", "chat", collectionTestLimits)

	require.NoError(t, err)
	require.NotNil(t, got.InferenceService)
	assert.Equal(t, parent.ObjectMeta, got.InferenceService.ObjectMeta)
	require.Len(t, got.InferenceReplicas, 2)
	assert.Equal(t, "chat-engine", got.InferenceReplicas[0].Name)
	assert.Equal(t, Completeness{ObservedPages: 2, ObservedItems: 2}, got.Completeness)

	actions := client.Actions()
	require.Len(t, actions, 3)
	get := actions[0].(ktesting.GetAction)
	assert.Equal(t, "prod", get.GetNamespace())
	assert.Equal(t, "chat", get.GetName())
	for index, action := range actions[1:] {
		list := action.(ktesting.ListAction)
		assert.Equal(t, "prod", list.GetNamespace())
		value, found := list.GetListRestrictions().Labels.RequiresExactMatch(constants.InferenceServicePodLabelKey)
		assert.True(t, found)
		assert.Equal(t, "chat", value)
		options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		assert.Equal(t, int64(2), options.Limit)
		assert.Equal(t, []string{"", "next"}[index], options.Continue)
	}

	// The result is an owned snapshot, not aliases into client responses.
	got.InferenceService.Name = "changed"
	got.InferenceReplicas[0].Name = "changed"
	assert.Equal(t, "chat", parent.Name)
}

func TestCollectNeverBuildsAnInvalidRelationshipLabelSelector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		length       int
		wantSelector bool
	}{
		{name: "maximum label value", length: 63, wantSelector: true},
		{name: "over label value limit", length: 64},
		{name: "maximum inference service name", length: 253},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			name := strings.Repeat("a", test.length)
			client := omefake.NewSimpleClientset(collectionISVC(name, "prod", "uid-parent"))
			client.PrependReactor("list", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
				options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
				if test.wantSelector {
					assert.Equal(t, constants.InferenceServicePodLabelKey+"="+name, options.LabelSelector)
				} else {
					assert.Empty(t, options.LabelSelector)
				}
				return true, &omev1beta1.InferenceReplicaList{}, nil
			})

			got, err := Collect(context.Background(), client.OmeV1beta1(), "prod", name, collectionTestLimits)

			require.NoError(t, err)
			assert.Empty(t, got.InferenceReplicas)
			require.Len(t, client.Actions(), 2)
		})
	}
}

func TestCollectLongNameScansBoundedlyAndKeepsOnlyExactRelationships(t *testing.T) {
	t.Parallel()

	parent := collectionISVC(strings.Repeat("a", 64), "prod", "uid-parent")
	relatedEngine := collectionRelatedIR(parent, "engine", "uid-engine")
	relatedEngine.Annotations = map[string]string{"source": "original"}
	relatedRouter := collectionRelatedIR(parent, "router", "uid-router")
	relatedRouter.Spec.Component = omev1beta1.RouterComponent

	hostileUnrelated := collectionIR("bad\n\x1b\u202esource", "prod", "uid-hostile")
	hostileUnrelated.Spec.ParentRef.Name = parent.Name
	wrongParent := collectionRelatedIR(parent, "wrong-parent", "uid-wrong-parent")
	wrongParent.Spec.ParentRef.Name = "someone-else"
	wrongOwner := collectionRelatedIR(parent, "wrong-owner", "uid-wrong-owner")
	wrongOwner.OwnerReferences[0].UID = "someone-else"
	wrongNamespace := collectionRelatedIR(parent, "wrong-namespace", "uid-wrong-namespace")
	wrongNamespace.Namespace = "other"

	client := omefake.NewSimpleClientset(parent)
	client.PrependReactor("list", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
		options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		assert.Empty(t, options.LabelSelector)
		switch options.Continue {
		case "":
			return true, &omev1beta1.InferenceReplicaList{
				ListMeta: metav1.ListMeta{Continue: "next"},
				Items:    []omev1beta1.InferenceReplica{hostileUnrelated, wrongParent, relatedEngine},
			}, nil
		case "next":
			return true, &omev1beta1.InferenceReplicaList{
				Items: []omev1beta1.InferenceReplica{relatedRouter, wrongOwner, wrongNamespace},
			}, nil
		default:
			return true, nil, errors.New("unexpected continue token")
		}
	})
	limits := collectionTestLimits
	limits.PageSize = 3
	limits.MaxItems = 6

	got, err := Collect(context.Background(), client.OmeV1beta1(), "prod", parent.Name, limits)

	require.NoError(t, err)
	assert.Equal(t, Completeness{ObservedPages: 2, ObservedItems: 6}, got.Completeness)
	require.Len(t, got.InferenceReplicas, 2)
	assert.Equal(t, []string{"engine", "router"}, []string{got.InferenceReplicas[0].Name, got.InferenceReplicas[1].Name})
	assert.NotContains(t, got.InferenceReplicas, hostileUnrelated)
	got.InferenceReplicas[0].Annotations["source"] = "changed"
	got.InferenceReplicas[0].OwnerReferences[0].UID = "changed"
	assert.Equal(t, "original", relatedEngine.Annotations["source"])
	assert.Equal(t, types.UID("uid-parent"), relatedEngine.OwnerReferences[0].UID)
}

func TestCollectLongNameMarksBoundedNamespaceScanTruncated(t *testing.T) {
	t.Parallel()

	parent := collectionISVC(strings.Repeat("a", 64), "prod", "uid-parent")
	related := collectionRelatedIR(parent, "engine", "uid-engine")
	unrelated := collectionRelatedIR(parent, "other", "uid-other")
	unrelated.OwnerReferences[0].Name = "someone-else"
	client := omefake.NewSimpleClientset(parent)
	client.PrependReactor("list", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
		options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		assert.Empty(t, options.LabelSelector)
		return true, &omev1beta1.InferenceReplicaList{
			ListMeta: metav1.ListMeta{Continue: "more"},
			Items:    []omev1beta1.InferenceReplica{related, unrelated},
		}, nil
	})
	limits := collectionTestLimits
	limits.MaxItems = 2

	got, err := Collect(context.Background(), client.OmeV1beta1(), "prod", parent.Name, limits)

	require.NoError(t, err)
	assert.Equal(t, Completeness{ObservedPages: 1, ObservedItems: 2, Truncated: true}, got.Completeness)
	require.Len(t, got.InferenceReplicas, 1)
	assert.Equal(t, "engine", got.InferenceReplicas[0].Name)
}

func TestCollectMarksBoundedReplicaPrefixTruncated(t *testing.T) {
	t.Parallel()

	parent := collectionISVC("chat", "prod", "uid-chat")
	client := omefake.NewSimpleClientset(parent)
	client.PrependReactor("list", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, &omev1beta1.InferenceReplicaList{
			ListMeta: metav1.ListMeta{Continue: "more"},
			Items: []omev1beta1.InferenceReplica{
				collectionIR("chat-engine", "prod", "ir-engine"),
				collectionIR("chat-router", "prod", "ir-router"),
			},
		}, nil
	})
	limits := collectionTestLimits
	limits.MaxItems = 2

	got, err := Collect(context.Background(), client.OmeV1beta1(), "prod", "chat", limits)

	require.NoError(t, err)
	assert.True(t, got.Completeness.Truncated)
	assert.Equal(t, Completeness{ObservedPages: 1, ObservedItems: 2, Truncated: true}, got.Completeness)
	require.Len(t, client.Actions(), 2)
}

func TestCollectRejectsUnboundParentResponsesBeforeListing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		object *omev1beta1.InferenceService
		want   error
	}{
		{name: "nil", want: ErrInferenceServiceRequired},
		{name: "name", object: collectionISVC("other", "prod", "uid"), want: ErrInferenceServiceNameMismatch},
		{name: "namespace", object: collectionISVC("chat", "other", "uid"), want: ErrInferenceServiceNamespaceMismatch},
		{name: "uid missing", object: collectionISVC("chat", "prod", ""), want: ErrInferenceServiceUIDMissing},
		{name: "uid hostile", object: collectionISVC("chat", "prod", "uid\nsecret"), want: ErrInferenceServiceUIDInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := omefake.NewSimpleClientset()
			client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
				if test.object == nil {
					var typedNil *omev1beta1.InferenceService
					return true, typedNil, nil
				}
				return true, test.object.DeepCopy(), nil
			})

			got, err := Collect(context.Background(), client.OmeV1beta1(), "prod", "chat", collectionTestLimits)

			require.ErrorIs(t, err, test.want)
			assert.Nil(t, got.InferenceService)
			assert.Empty(t, got.InferenceReplicas)
			require.Len(t, client.Actions(), 1)
		})
	}
}

func TestCollectPropagatesCancellationAndListErrorsWithoutPartialSnapshot(t *testing.T) {
	t.Parallel()

	t.Run("pre-canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		client := omefake.NewSimpleClientset(collectionISVC("chat", "prod", "uid-chat"))

		got, err := Collect(ctx, client.OmeV1beta1(), "prod", "chat", collectionTestLimits)

		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, got.InferenceService)
		assert.Empty(t, client.Actions())
	})

	t.Run("list error", func(t *testing.T) {
		boom := errors.New("list failed")
		client := omefake.NewSimpleClientset(collectionISVC("chat", "prod", "uid-chat"))
		client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, boom
		})

		got, err := Collect(context.Background(), client.OmeV1beta1(), "prod", "chat", collectionTestLimits)

		require.ErrorIs(t, err, boom)
		assert.Nil(t, got.InferenceService)
		assert.Empty(t, got.InferenceReplicas)
	})
}

func TestCollectLongNamePropagatesCancellationAndListErrorsWithoutPartialSnapshot(t *testing.T) {
	t.Parallel()

	name := strings.Repeat("a", 64)
	t.Run("canceled namespace scan", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		fake := omefake.NewSimpleClientset(collectionISVC(name, "prod", "uid-parent"))
		client := collectionListClient{
			OmeV1beta1Interface: fake.OmeV1beta1(),
			list: func(requestCtx context.Context, options metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
				assert.Empty(t, options.LabelSelector)
				cancel()
				return nil, requestCtx.Err()
			},
		}

		got, err := Collect(ctx, client, "prod", name, collectionTestLimits)

		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, got.InferenceService)
		assert.Empty(t, got.InferenceReplicas)
	})

	t.Run("namespace scan client error", func(t *testing.T) {
		boom := errors.New("namespace scan failed")
		fake := omefake.NewSimpleClientset(collectionISVC(name, "prod", "uid-parent"))
		client := collectionListClient{
			OmeV1beta1Interface: fake.OmeV1beta1(),
			list: func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
				return nil, boom
			},
		}

		got, err := Collect(context.Background(), client, "prod", name, collectionTestLimits)

		require.ErrorIs(t, err, boom)
		assert.Nil(t, got.InferenceService)
		assert.Empty(t, got.InferenceReplicas)
	})
}

func TestCollectRejectsMissingClientWithoutAttemptingARead(t *testing.T) {
	t.Parallel()

	got, err := Collect(context.Background(), nil, "prod", "chat", collectionTestLimits)

	require.ErrorIs(t, err, ErrClientRequired)
	assert.Nil(t, got.InferenceService)
	assert.Empty(t, got.InferenceReplicas)
}

func TestCollectPropagatesGetAndMissingListResponsesWithoutPartialSnapshot(t *testing.T) {
	t.Parallel()

	t.Run("get error", func(t *testing.T) {
		boom := errors.New("get failed")
		client := omefake.NewSimpleClientset()
		client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, boom
		})

		got, err := Collect(context.Background(), client.OmeV1beta1(), "prod", "chat", collectionTestLimits)

		require.ErrorIs(t, err, boom)
		assert.Nil(t, got.InferenceService)
		assert.Empty(t, got.InferenceReplicas)
		require.Len(t, client.Actions(), 1)
	})

	t.Run("missing list response", func(t *testing.T) {
		fake := omefake.NewSimpleClientset(collectionISVC("chat", "prod", "uid-chat"))
		client := collectionMissingListClient{OmeV1beta1Interface: fake.OmeV1beta1()}

		got, err := Collect(context.Background(), client, "prod", "chat", collectionTestLimits)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty InferenceReplica list response")
		assert.Nil(t, got.InferenceService)
		assert.Empty(t, got.InferenceReplicas)
		require.Len(t, fake.Actions(), 1)
	})
}

func TestCollectRejectsInvalidLimitsBeforeAnyRead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*paging.Limits)
		want   string
	}{
		{name: "page size", mutate: func(limits *paging.Limits) { limits.PageSize = 0 }, want: "page size must be positive"},
		{name: "item limit", mutate: func(limits *paging.Limits) { limits.MaxItems = 0 }, want: "item limit must be positive"},
		{name: "page limit", mutate: func(limits *paging.Limits) { limits.MaxPages = 0 }, want: "page limit must be positive"},
		{name: "request timeout", mutate: func(limits *paging.Limits) { limits.RequestTimeout = 0 }, want: "request timeout must be positive"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := omefake.NewSimpleClientset(collectionISVC("chat", "prod", "uid-chat"))
			limits := collectionTestLimits
			test.mutate(&limits)

			_, err := Collect(context.Background(), client.OmeV1beta1(), "prod", "chat", limits)

			require.EqualError(t, err, test.want)
			assert.Empty(t, client.Actions())
		})
	}
}

func TestCollectRejectsInvalidRequestIdentityBeforeAnyRead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		namespace string
		isvc      string
		want      error
	}{
		{name: "empty namespace", isvc: "chat", want: ErrInferenceServiceNamespaceInvalid},
		{name: "invalid namespace", namespace: "Bad_Namespace", isvc: "chat", want: ErrInferenceServiceNamespaceInvalid},
		{name: "empty name", namespace: "prod", want: ErrInferenceServiceNameInvalid},
		{name: "invalid name", namespace: "prod", isvc: "Bad_Name", want: ErrInferenceServiceNameInvalid},
		{name: "name too long", namespace: "prod", isvc: strings.Repeat("a", 254), want: ErrInferenceServiceNameInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client := omefake.NewSimpleClientset()

			_, err := Collect(context.Background(), client.OmeV1beta1(), test.namespace, test.isvc, collectionTestLimits)

			require.ErrorIs(t, err, test.want)
			assert.Empty(t, client.Actions())
		})
	}
}

func collectionISVC(name, namespace, uid string) *omev1beta1.InferenceService {
	return &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: namespace, UID: types.UID(uid), Generation: 4,
	}}
}

func collectionIR(name, namespace, uid string) omev1beta1.InferenceReplica {
	return omev1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: namespace, UID: types.UID(uid),
		Labels: map[string]string{constants.InferenceServicePodLabelKey: "chat"},
	}}
}

func collectionRelatedIR(parent *omev1beta1.InferenceService, name, uid string) omev1beta1.InferenceReplica {
	controller := true
	return omev1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: parent.Namespace, UID: types.UID(uid), Generation: 2,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: omev1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
				Name: parent.Name, UID: parent.UID, Controller: &controller,
			}},
		},
		Spec: omev1beta1.InferenceReplicaSpec{
			ParentRef: omev1beta1.ParentReference{Name: parent.Name}, Component: omev1beta1.EngineComponent,
		},
	}
}

type collectionMissingListClient struct {
	omeclient.OmeV1beta1Interface
}

func (collectionMissingListClient) InferenceReplicas(string) omeclient.InferenceReplicaInterface {
	return collectionMissingInferenceReplicas{}
}

type collectionMissingInferenceReplicas struct {
	omeclient.InferenceReplicaInterface
}

func (collectionMissingInferenceReplicas) List(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
	return nil, nil
}

type collectionListClient struct {
	omeclient.OmeV1beta1Interface
	list func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error)
}

func (c collectionListClient) InferenceReplicas(namespace string) omeclient.InferenceReplicaInterface {
	return collectionListInferenceReplicas{
		InferenceReplicaInterface: c.OmeV1beta1Interface.InferenceReplicas(namespace),
		list:                      c.list,
	}
}

type collectionListInferenceReplicas struct {
	omeclient.InferenceReplicaInterface
	list func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error)
}

func (c collectionListInferenceReplicas) List(ctx context.Context, options metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
	return c.list(ctx, options)
}
