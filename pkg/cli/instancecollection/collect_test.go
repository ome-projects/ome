package instancecollection_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/instancecollection"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestCollectRelatedPagesWithExactSelectorAndRejectsUnboundObjects(t *testing.T) {
	t.Parallel()

	isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("isvc-uid"),
	}}
	validEngine := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	wrongParent := relatedReplica(isvc, "wrong-parent", omev1beta1.EngineComponent)
	wrongParent.Spec.ParentRef.Name = "other"
	validDecoder := relatedReplica(isvc, "chat-decoder", omev1beta1.DecoderComponent)
	wrongOwner := relatedReplica(isvc, "wrong-owner", omev1beta1.RouterComponent)
	wrongOwner.OwnerReferences[0].UID = types.UID("other-uid")
	badComponent := relatedReplica(isvc, "bad-component", omev1beta1.ComponentType("future"))

	lister := &pagedLister{pages: map[string]*omev1beta1.InferenceReplicaList{
		"": {
			ListMeta: metav1.ListMeta{Continue: "next"},
			Items:    []omev1beta1.InferenceReplica{validEngine, wrongParent},
		},
		"next": {
			Items: []omev1beta1.InferenceReplica{validDecoder, wrongOwner, badComponent},
		},
	}}

	got, err := instancecollection.CollectRelated(
		context.Background(), lister, isvc,
		instancecollection.Limits{
			Paging:        paging.Limits{PageSize: 2, MaxItems: 10, MaxPages: 5, RequestTimeout: time.Second},
			MaxStatusRows: 100,
		},
	)
	require.NoError(t, err)
	require.Len(t, lister.options, 2)
	for _, options := range lister.options {
		assert.Equal(t, constants.InferenceServiceLabel+"=chat", options.LabelSelector)
		assert.Equal(t, int64(2), options.Limit)
	}
	assert.Empty(t, lister.options[0].Continue)
	assert.Equal(t, "next", lister.options[1].Continue)

	require.Len(t, got.Items, 2)
	assert.Equal(t, []string{"chat-engine", "chat-decoder"}, []string{got.Items[0].Name, got.Items[1].Name})
	assert.Equal(t, []instancecollection.Rejection{
		{Name: "wrong-parent", Reason: instancecollection.RejectionParentReference},
		{Name: "wrong-owner", Reason: instancecollection.RejectionOwnerReference},
		{Name: "bad-component", Reason: instancecollection.RejectionComponent},
	}, got.Rejected)
	assert.Equal(t, 2, got.Pages)
	assert.False(t, got.Truncated)
}

func TestCollectRelatedRejectsSuccessfulResponseAfterRequestCancellation(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	ctx, cancel := context.WithCancel(context.Background())
	lister := listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
		cancel()
		return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{
			relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent),
		}}, nil
	})

	got, err := instancecollection.CollectRelated(ctx, lister, isvc, collectionLimits())

	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, got.Items)
	assert.Equal(t, 1, got.Pages)
}

func TestCollectRelatedReturnsDefensiveCopies(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	source := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	source.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{
		Index: 0, Phase: omev1beta1.OMENativeInstanceReady, ReadyPodCount: 1,
	}}
	lister := listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
		return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{source}}, nil
	})

	got, err := instancecollection.CollectRelated(context.Background(), lister, isvc, collectionLimits())
	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	assert.Zero(t, got.Items[0].Status.InstanceStatuses[0].ReadyPodCount,
		"the non-persisted API compatibility counter must not cross the collection boundary")

	got.Items[0].Labels[constants.InferenceServiceLabel] = "output-mutated"
	got.Items[0].Status.InstanceStatuses[0].Index = 9
	assert.Equal(t, isvc.Name, source.Labels[constants.InferenceServiceLabel])
	assert.Equal(t, int32(0), source.Status.InstanceStatuses[0].Index)

	source.Labels[constants.InferenceServiceLabel] = "source-mutated"
	source.Status.InstanceStatuses[0].Index = 7
	assert.Equal(t, "output-mutated", got.Items[0].Labels[constants.InferenceServiceLabel])
	assert.Equal(t, int32(9), got.Items[0].Status.InstanceStatuses[0].Index)
}

func TestCollectRelatedBoundsNestedStatusCopies(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	large := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	large.Status.Replicas = 10_000
	large.Status.InstanceStatuses = make([]omev1beta1.OMENativeInstanceStatus, 10_000)
	for index := range large.Status.InstanceStatuses {
		large.Status.InstanceStatuses[index] = omev1beta1.OMENativeInstanceStatus{
			Index: int32(index), Phase: omev1beta1.OMENativeInstanceReady,
			Operation: &omev1beta1.InstanceOperation{
				HintTargetNodes: []string{"SECRET-NESTED-OPAQUE-VALUE"},
			},
		}
	}
	small := relatedReplica(isvc, "chat-decoder", omev1beta1.DecoderComponent)
	small.Status.Replicas = 1
	small.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{
		Index: 0, Phase: omev1beta1.OMENativeInstanceReady,
	}}
	limits := collectionLimits()
	limits.MaxStatusRows = 1
	collect := func(items []omev1beta1.InferenceReplica) instancecollection.Result {
		lister := listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
			return &omev1beta1.InferenceReplicaList{Items: items}, nil
		})
		got, err := instancecollection.CollectRelated(context.Background(), lister, isvc, limits)
		require.NoError(t, err)
		return got
	}

	got := collect([]omev1beta1.InferenceReplica{small, large})
	assert.Equal(t, got, collect([]omev1beta1.InferenceReplica{large, small}),
		"nested work-budget selection must not inherit API response order")
	require.Len(t, got.Items, 2)
	assert.Equal(t, []string{"chat-engine", "chat-decoder"}, []string{got.Items[0].Name, got.Items[1].Name})
	assert.Empty(t, got.Items[0].Status.InstanceStatuses,
		"an oversized nested list must be dropped before any deep copy")
	require.Len(t, got.Items[1].Status.InstanceStatuses, 1)
	assert.Equal(t, []instancecollection.StatusRowsTruncation{{
		Name: "chat-engine", Component: omev1beta1.EngineComponent,
	}}, got.StatusRowsTruncated)
	encoded, marshalErr := json.Marshal(got.Items[0])
	require.NoError(t, marshalErr)
	assert.NotContains(t, string(encoded), "SECRET-NESTED-OPAQUE-VALUE")
}

func TestCollectRelatedRequiresExactNonemptyControllerUID(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	ir := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	ir.OwnerReferences[0].UID = "different"
	lister := listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
		return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{ir}}, nil
	})

	withUID, err := instancecollection.CollectRelated(context.Background(), lister, isvc, collectionLimits())
	require.NoError(t, err)
	assert.Empty(t, withUID.Items)
	require.Len(t, withUID.Rejected, 1)
	assert.Equal(t, instancecollection.RejectionOwnerReference, withUID.Rejected[0].Reason)

	isvcWithoutUID := isvc.DeepCopy()
	isvcWithoutUID.UID = ""
	_, err = instancecollection.CollectRelated(context.Background(), panicLister{}, isvcWithoutUID, collectionLimits())
	require.ErrorIs(t, err, instancecollection.ErrInferenceServiceIdentityInvalid)
}

func TestCollectRelatedRejectsEveryIdentityMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceReplica)
		want   instancecollection.RejectionReason
	}{
		{name: "empty name", mutate: func(ir *omev1beta1.InferenceReplica) { ir.Name = "" }, want: instancecollection.RejectionMetadata},
		{name: "unsafe name", mutate: func(ir *omev1beta1.InferenceReplica) { ir.Name = "bad\u202e\nname" }, want: instancecollection.RejectionMetadata},
		{name: "namespace", mutate: func(ir *omev1beta1.InferenceReplica) { ir.Namespace = "other" }, want: instancecollection.RejectionMetadata},
		{name: "empty uid", mutate: func(ir *omev1beta1.InferenceReplica) { ir.UID = "" }, want: instancecollection.RejectionMetadata},
		{name: "unsafe uid", mutate: func(ir *omev1beta1.InferenceReplica) { ir.UID = "uid\u202e\nSECRET" }, want: instancecollection.RejectionMetadata},
		{name: "label", mutate: func(ir *omev1beta1.InferenceReplica) { ir.Labels[constants.InferenceServiceLabel] = "other" }, want: instancecollection.RejectionLabel},
		{name: "parent", mutate: func(ir *omev1beta1.InferenceReplica) { ir.Spec.ParentRef.Name = "other" }, want: instancecollection.RejectionParentReference},
		{name: "owner api version", mutate: func(ir *omev1beta1.InferenceReplica) { ir.OwnerReferences[0].APIVersion = "ome.io/v1" }, want: instancecollection.RejectionOwnerReference},
		{name: "owner kind", mutate: func(ir *omev1beta1.InferenceReplica) { ir.OwnerReferences[0].Kind = "Other" }, want: instancecollection.RejectionOwnerReference},
		{name: "owner name", mutate: func(ir *omev1beta1.InferenceReplica) { ir.OwnerReferences[0].Name = "other" }, want: instancecollection.RejectionOwnerReference},
		{name: "owner uid", mutate: func(ir *omev1beta1.InferenceReplica) { ir.OwnerReferences[0].UID = "other" }, want: instancecollection.RejectionOwnerReference},
		{name: "owner not controller", mutate: func(ir *omev1beta1.InferenceReplica) { ir.OwnerReferences[0].Controller = nil }, want: instancecollection.RejectionOwnerReference},
		{name: "multiple controllers", mutate: func(ir *omev1beta1.InferenceReplica) {
			ir.OwnerReferences = append(ir.OwnerReferences, ir.OwnerReferences[0])
		}, want: instancecollection.RejectionOwnerReference},
		{name: "component", mutate: func(ir *omev1beta1.InferenceReplica) { ir.Spec.Component = "future" }, want: instancecollection.RejectionComponent},
		{name: "component label", mutate: func(ir *omev1beta1.InferenceReplica) {
			ir.Labels[constants.OMEComponentLabel] = "router"
		}, want: instancecollection.RejectionComponent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			isvc := collectionISVC()
			ir := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
			test.mutate(&ir)
			lister := listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
				return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{ir}}, nil
			})

			got, err := instancecollection.CollectRelated(context.Background(), lister, isvc, collectionLimits())
			require.NoError(t, err)
			assert.Empty(t, got.Items)
			require.Len(t, got.Rejected, 1)
			assert.Equal(t, test.want, got.Rejected[0].Reason)
			if test.name == "unsafe name" {
				assert.Equal(t, "INVALID", got.Rejected[0].Name)
				assert.NotContains(t, got.Rejected[0].Name, "SECRET")
			}
		})
	}
}

func TestCollectRelatedPreservesBoundedProgressOnLaterPageFailure(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	wantErr := errors.New("second page failed")
	calls := 0
	lister := listerFunc(func(_ context.Context, options metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
		calls++
		if options.Continue == "" {
			return &omev1beta1.InferenceReplicaList{
				ListMeta: metav1.ListMeta{Continue: "next"},
				Items: []omev1beta1.InferenceReplica{
					relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent),
				},
			}, nil
		}
		return nil, wantErr
	})

	got, err := instancecollection.CollectRelated(context.Background(), lister, isvc, collectionLimits())

	require.ErrorIs(t, err, wantErr)
	require.Len(t, got.Items, 1)
	assert.Equal(t, "chat-engine", got.Items[0].Name)
	assert.Equal(t, 2, got.Pages)
	assert.Equal(t, 2, calls)
}

func TestCollectRelatedReportsTruncation(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	lister := listerFunc(func(_ context.Context, options metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
		return &omev1beta1.InferenceReplicaList{
			ListMeta: metav1.ListMeta{Continue: options.Continue + "next"},
			Items: []omev1beta1.InferenceReplica{
				relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent),
			},
		}, nil
	})
	limits := collectionLimits()
	limits.Paging.MaxItems = 1

	got, err := instancecollection.CollectRelated(context.Background(), lister, isvc, limits)

	require.NoError(t, err)
	assert.True(t, got.Truncated)
	require.Len(t, got.Items, 1)
}

func TestCollectRelatedRejectsInvalidInputsWithoutListing(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	tests := []struct {
		name   string
		lister instancecollection.Lister
		isvc   *omev1beta1.InferenceService
		limits instancecollection.Limits
		want   error
	}{
		{name: "nil primary", lister: panicLister{}, limits: collectionLimits(), want: instancecollection.ErrInferenceServiceRequired},
		{name: "empty name", lister: panicLister{}, isvc: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "prod"}}, limits: collectionLimits(), want: instancecollection.ErrInferenceServiceNameRequired},
		{name: "empty namespace", lister: panicLister{}, isvc: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat"}}, limits: collectionLimits(), want: instancecollection.ErrInferenceServiceNamespaceRequired},
		{name: "unsafe name", lister: panicLister{}, isvc: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "Bad_Name", Namespace: "prod", UID: "isvc-uid"}}, limits: collectionLimits(), want: instancecollection.ErrInferenceServiceIdentityInvalid},
		{name: "unsafe namespace", lister: panicLister{}, isvc: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "Bad_Namespace", UID: "isvc-uid"}}, limits: collectionLimits(), want: instancecollection.ErrInferenceServiceIdentityInvalid},
		{name: "empty uid", lister: panicLister{}, isvc: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod"}}, limits: collectionLimits(), want: instancecollection.ErrInferenceServiceIdentityInvalid},
		{name: "unsafe uid", lister: panicLister{}, isvc: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: "uid\u202e\nSECRET"}}, limits: collectionLimits(), want: instancecollection.ErrInferenceServiceIdentityInvalid},
		{name: "nil lister", isvc: isvc, limits: collectionLimits(), want: instancecollection.ErrListerRequired},
		{name: "invalid status row limit", lister: panicLister{}, isvc: isvc, limits: instancecollection.Limits{Paging: collectionLimits().Paging}, want: instancecollection.ErrMaxStatusRowsInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := instancecollection.CollectRelated(context.Background(), test.lister, test.isvc, test.limits)
			require.ErrorIs(t, err, test.want)
		})
	}
}

func TestCollectRelatedReturnsNilResponseAndLimitErrors(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	t.Run("nil response", func(t *testing.T) {
		_, err := instancecollection.CollectRelated(
			context.Background(),
			listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
				return nil, nil
			}),
			isvc, collectionLimits(),
		)
		require.ErrorContains(t, err, "InferenceReplica list returned nil")
	})
	t.Run("invalid limits", func(t *testing.T) {
		_, err := instancecollection.CollectRelated(context.Background(), panicLister{}, isvc, instancecollection.Limits{MaxStatusRows: 1})
		require.ErrorContains(t, err, "page size must be positive")
	})
}

type pagedLister struct {
	pages   map[string]*omev1beta1.InferenceReplicaList
	options []metav1.ListOptions
}

type listerFunc func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error)

func (f listerFunc) List(ctx context.Context, options metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
	return f(ctx, options)
}

type panicLister struct{}

func (panicLister) List(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
	panic("List must not be called")
}

func (l *pagedLister) List(_ context.Context, options metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
	l.options = append(l.options, options)
	return l.pages[options.Continue].DeepCopy(), nil
}

func relatedReplica(
	isvc *omev1beta1.InferenceService,
	name string,
	component omev1beta1.ComponentType,
) omev1beta1.InferenceReplica {
	controller := true
	return omev1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: isvc.Namespace, UID: types.UID(name + "-uid"),
			Labels: map[string]string{
				constants.InferenceServiceLabel: isvc.Name,
				constants.OMEComponentLabel:     string(component),
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: omev1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
				Name: isvc.Name, UID: isvc.UID, Controller: &controller,
			}},
		},
		Spec: omev1beta1.InferenceReplicaSpec{
			ParentRef: omev1beta1.ParentReference{Name: isvc.Name}, Component: component,
		},
	}
}

func collectionISVC() *omev1beta1.InferenceService {
	return &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("isvc-uid"),
	}}
}

func collectionLimits() instancecollection.Limits {
	return instancecollection.Limits{
		Paging:        paging.Limits{PageSize: 2, MaxItems: 10, MaxPages: 5, RequestTimeout: time.Second},
		MaxStatusRows: 100,
	}
}
