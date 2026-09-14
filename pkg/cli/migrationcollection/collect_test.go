package migrationcollection

import (
	"context"
	"errors"
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

func TestCollectRejectsInvalidLimitsBeforeAnyRead(t *testing.T) {
	t.Parallel()

	client := omefake.NewSimpleClientset(collectionISVC("chat", "prod", "uid-chat"))
	_, err := Collect(context.Background(), client.OmeV1beta1(), "prod", "chat", paging.Limits{})
	require.Error(t, err)
	assert.Empty(t, client.Actions())
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
