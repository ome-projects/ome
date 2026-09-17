package instancecollection_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/instancecollection"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
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

func TestCollectRelatedLongParentNameUsesBoundedExactRelationshipScan(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	isvc.Name = strings.Repeat("a", 64)
	related := relatedReplica(isvc, "engine", omev1beta1.EngineComponent)
	delete(related.Labels, constants.InferenceServiceLabel)
	wrongParent := *related.DeepCopy()
	wrongParent.Name = "wrong-parent"
	wrongParent.UID = "wrong-parent-uid"
	wrongParent.Spec.ParentRef.Name = "other"
	wrongOwner := *related.DeepCopy()
	wrongOwner.Name = "wrong-owner"
	wrongOwner.UID = "wrong-owner-uid"
	wrongOwner.OwnerReferences[0].UID = "other"
	lister := &pagedLister{pages: map[string]*omev1beta1.InferenceReplicaList{
		"": {
			ListMeta: metav1.ListMeta{Continue: "next"},
			Items:    []omev1beta1.InferenceReplica{wrongParent, related},
		},
		"next": {Items: []omev1beta1.InferenceReplica{wrongOwner}},
	}}
	limits := collectionLimits()
	limits.Paging.PageSize = 2

	got, err := instancecollection.CollectRelated(context.Background(), lister, isvc, limits)

	require.NoError(t, err)
	require.Len(t, lister.options, 2)
	assert.Empty(t, lister.options[0].LabelSelector)
	assert.Empty(t, lister.options[1].LabelSelector)
	require.Len(t, got.Items, 1)
	assert.Equal(t, "engine", got.Items[0].Name)
	assert.Empty(t, got.Items[0].Labels[constants.InferenceServiceLabel])
	assert.Equal(t, []instancecollection.Rejection{
		{Name: "wrong-parent", Reason: instancecollection.RejectionParentReference},
		{Name: "wrong-owner", Reason: instancecollection.RejectionOwnerReference},
	}, got.Rejected)
}

func TestCollectRelatedLongParentScanSkipsUnrelatedButRejectsTargetClaim(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	isvc.Name = strings.Repeat("a", 64)
	related := relatedReplica(isvc, "engine", omev1beta1.EngineComponent)
	delete(related.Labels, constants.InferenceServiceLabel)
	badOwner := *related.DeepCopy()
	badOwner.Name = "bad-owner"
	badOwner.UID = "bad-owner-uid"
	badOwner.OwnerReferences[0].UID = "wrong-uid"
	otherISVC := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "other", Namespace: isvc.Namespace, UID: "other-uid",
	}}
	unrelated := relatedReplica(otherISVC, "other-engine", omev1beta1.EngineComponent)

	got, err := instancecollection.CollectRelated(
		context.Background(),
		listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
			return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{
				unrelated, badOwner, related,
			}}, nil
		}),
		isvc,
		collectionLimits(),
	)

	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	assert.Equal(t, "engine", got.Items[0].Name)
	assert.Equal(t, []instancecollection.Rejection{{
		Name: "bad-owner", Reason: instancecollection.RejectionOwnerReference,
	}}, got.Rejected)
}

func TestCollectRelatedLongParentScanTreatsExactOwnerUIDAsTargetClaim(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	isvc.Name = strings.Repeat("a", 64)
	related := relatedReplica(isvc, "engine", omev1beta1.EngineComponent)
	delete(related.Labels, constants.InferenceServiceLabel)
	otherISVC := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "other", Namespace: isvc.Namespace, UID: "other-uid",
	}}
	suspicious := relatedReplica(otherISVC, "suspicious-engine", omev1beta1.EngineComponent)
	suspicious.OwnerReferences[0].APIVersion = "malformed/v1"
	suspicious.OwnerReferences[0].Kind = "Other"
	suspicious.OwnerReferences[0].Name = "other"
	suspicious.OwnerReferences[0].UID = isvc.UID

	got, err := instancecollection.CollectRelated(
		context.Background(),
		listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
			return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{
				suspicious, related,
			}}, nil
		}),
		isvc,
		collectionLimits(),
	)

	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	assert.Equal(t, "engine", got.Items[0].Name)
	assert.Equal(t, []instancecollection.Rejection{{
		Name: "suspicious-engine", Reason: instancecollection.RejectionParentReference,
	}}, got.Rejected)
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

func TestCollectRelatedPreservesDefensiveDeletionTimestamp(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	source := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	deletingAt := metav1.NewTime(time.Date(2026, 9, 14, 22, 30, 0, 0, time.UTC))
	source.DeletionTimestamp = &deletingAt

	got, err := instancecollection.CollectRelated(
		context.Background(),
		listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
			return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{source}}, nil
		}),
		isvc,
		collectionLimits(),
	)

	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	require.NotNil(t, got.Items[0].DeletionTimestamp)
	assert.Equal(t, deletingAt.Time, got.Items[0].DeletionTimestamp.Time)
	got.Items[0].DeletionTimestamp.Time = time.Time{}
	assert.Equal(t, deletingAt.Time, source.DeletionTimestamp.Time)
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

func TestCollectRelatedColumnarCopiesLogicalRowsAndSelectedDetails(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	source := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	rows := []omev1beta1.OMENativeInstanceStatus{
		{
			Index: 2, Phase: omev1beta1.OMENativeInstanceUpdating,
			Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Healthy"}},
		},
		{Index: 0, Phase: omev1beta1.OMENativeInstanceReady},
	}
	columns, err := irstatus.EncodeColumns(rows, 2)
	require.NoError(t, err)
	encoding := omev1beta1.InstanceStatusEncodingColumnarV2
	source.Status.Replicas = 2
	source.Status.InstanceStatusEncoding = &encoding
	source.Status.InstanceStatusColumns = columns
	original := source.DeepCopy()
	limits := collectionLimits()
	limits.MaxStatusRows = 2
	limits.Details = instancecollection.DetailLimits{
		SelectedComponent: omev1beta1.EngineComponent, SelectedIndex: 2,
		MaxConditions: 4, MaxScannedConditions: 4, MaxNodeHints: 4, MaxScannedNodeHints: 4,
	}

	got, err := instancecollection.CollectRelated(context.Background(), listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
		return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{source}}, nil
	}), isvc, limits)

	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	assert.Empty(t, got.StatusRowsTruncated)
	require.Len(t, got.Items[0].Status.InstanceStatuses, 2)
	assert.Equal(t, []int32{2, 0}, []int32{
		got.Items[0].Status.InstanceStatuses[0].Index,
		got.Items[0].Status.InstanceStatuses[1].Index,
	})
	assert.Equal(t, omev1beta1.OMENativeInstanceUpdating, got.Items[0].Status.InstanceStatuses[0].Phase)
	assert.Equal(t, omev1beta1.OMENativeInstanceReady, got.Items[0].Status.InstanceStatuses[1].Phase)
	require.Len(t, got.Items[0].Status.InstanceStatuses[0].Conditions, 1)
	assert.Equal(t, "Ready", got.Items[0].Status.InstanceStatuses[0].Conditions[0].Type)
	assert.Nil(t, got.Items[0].Status.InstanceStatusEncoding)
	assert.Nil(t, got.Items[0].Status.InstanceStatusColumns)
	assert.Equal(t, original, source.DeepCopy(), "collection must not mutate the API object")
}

func TestCollectRelatedColumnarRejectsMalformedRepresentations(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	rows := []omev1beta1.OMENativeInstanceStatus{{Index: 0, Phase: omev1beta1.OMENativeInstanceReady}}
	columns, err := irstatus.EncodeColumns(rows, 1)
	require.NoError(t, err)
	columnar := omev1beta1.InstanceStatusEncodingColumnarV2
	unknown := omev1beta1.InstanceStatusEncoding("SECRET-FUTURE-ENCODING")
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceReplica)
	}{
		{name: "unknown encoding", mutate: func(ir *omev1beta1.InferenceReplica) {
			ir.Status.InstanceStatusEncoding = &unknown
		}},
		{name: "marked with dense rows", mutate: func(ir *omev1beta1.InferenceReplica) {
			ir.Status.InstanceStatuses = rows
		}},
		{name: "missing columns", mutate: func(ir *omev1beta1.InferenceReplica) {
			ir.Status.InstanceStatusColumns = nil
		}},
		{name: "bad coverage", mutate: func(ir *omev1beta1.InferenceReplica) {
			ir.Status.InstanceStatusColumns.Phases = nil
		}},
		{name: "group count exceeds members", mutate: func(ir *omev1beta1.InferenceReplica) {
			ir.Status.InstanceStatusColumns.Phases = append(ir.Status.InstanceStatusColumns.Phases,
				omev1beta1.InstanceStatusPhaseGroup{Value: omev1beta1.OMENativeInstanceUpdating, Indexes: "0"})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			source := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
			source.Status.Replicas = 1
			source.Status.InstanceStatusEncoding = &columnar
			source.Status.InstanceStatusColumns = columns.DeepCopy()
			test.mutate(&source)
			limits := collectionLimits()
			limits.MaxStatusRows = 1
			got, collectErr := instancecollection.CollectRelated(context.Background(), listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
				return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{source}}, nil
			}), isvc, limits)
			require.Error(t, collectErr)
			assert.Equal(t, "instance collection status encoding is invalid", collectErr.Error())
			assert.Empty(t, got.Items)
			assert.NotContains(t, collectErr.Error(), "SECRET-FUTURE-ENCODING")
		})
	}
}

func TestCollectRelatedColumnarTruncatesAfterValidatedAggregateBudget(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	engine := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	engine.Status.Replicas = 1
	engine.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{Index: 0, Phase: omev1beta1.OMENativeInstanceReady}}
	decoder := relatedReplica(isvc, "chat-decoder", omev1beta1.DecoderComponent)
	decoder.Status.Replicas = 2
	columns, err := irstatus.EncodeColumns([]omev1beta1.OMENativeInstanceStatus{
		{Index: 2, Phase: omev1beta1.OMENativeInstanceReady},
		{Index: 1, Phase: omev1beta1.OMENativeInstanceReady},
	}, 2)
	require.NoError(t, err)
	encoding := omev1beta1.InstanceStatusEncodingColumnarV2
	decoder.Status.InstanceStatusEncoding = &encoding
	decoder.Status.InstanceStatusColumns = columns
	limits := collectionLimits()
	limits.MaxStatusRows = 2
	collect := func(items []omev1beta1.InferenceReplica) instancecollection.Result {
		got, collectErr := instancecollection.CollectRelated(context.Background(), listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
			return &omev1beta1.InferenceReplicaList{Items: items}, nil
		}), isvc, limits)
		require.NoError(t, collectErr)
		return got
	}

	got := collect([]omev1beta1.InferenceReplica{decoder, engine})
	assert.Equal(t, got, collect([]omev1beta1.InferenceReplica{engine, decoder}))
	require.Len(t, got.Items, 2)
	require.Len(t, got.Items[0].Status.InstanceStatuses, 1)
	assert.Empty(t, got.Items[1].Status.InstanceStatuses)
	assert.Equal(t, []instancecollection.StatusRowsTruncation{{
		Name: "chat-decoder", Component: omev1beta1.DecoderComponent,
	}}, got.StatusRowsTruncated)
}

func TestCollectRelatedColumnarTruncatesDefaultControllerCardinality(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	source := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	encoding := omev1beta1.InstanceStatusEncodingColumnarV2
	source.Status.InstanceStatusEncoding = &encoding
	source.Status.InstanceStatusColumns = &omev1beta1.InstanceStatusColumns{
		Members: "0-4999",
		Phases:  []omev1beta1.InstanceStatusPhaseGroup{{Value: omev1beta1.OMENativeInstanceReady, Indexes: "0-4999"}},
	}
	limits := collectionLimits()
	limits.MaxStatusRows = 1
	got, err := instancecollection.CollectRelated(context.Background(), listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
		return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{source}}, nil
	}), isvc, limits)

	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	assert.Empty(t, got.Items[0].Status.InstanceStatuses)
	assert.Equal(t, []instancecollection.StatusRowsTruncation{{Name: source.Name, Component: source.Spec.Component}}, got.StatusRowsTruncated)
}

func TestCollectRelatedColumnarRejectsExpansionBeyondFixedCeiling(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	source := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	encoding := omev1beta1.InstanceStatusEncodingColumnarV2
	source.Status.Replicas = 20_001
	source.Status.InstanceStatusEncoding = &encoding
	source.Status.InstanceStatusColumns = &omev1beta1.InstanceStatusColumns{
		Members: "0-20000",
		Phases:  []omev1beta1.InstanceStatusPhaseGroup{{Value: omev1beta1.OMENativeInstanceReady, Indexes: "0-20000"}},
	}
	original := source.DeepCopy()
	limits := collectionLimits()
	limits.MaxStatusRows = 20_001 // A caller's larger output budget must not raise the decoder's cap.

	got, err := instancecollection.CollectRelated(context.Background(), listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
		return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{source}}, nil
	}), isvc, limits)

	require.ErrorIs(t, err, instancecollection.ErrStatusEncodingInvalid)
	assert.Empty(t, got.Items)
	assert.Empty(t, got.StatusEncodings)
	assert.Equal(t, original, source.DeepCopy(), "decoding must not mutate the API object")
}

func TestCollectRelatedCopiesSelectedLifecycleAndRelevantMigrationsOnly(t *testing.T) {
	t.Parallel()
	isvc := collectionISVC()
	ir := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	ready := metav1.NewTime(time.Date(2026, 9, 14, 20, 0, 0, 0, time.UTC))
	ir.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{
		{Index: 2, Phase: omev1beta1.OMENativeInstanceReady, ReadySince: &ready, ActiveOrdinal: 1},
		{Index: 3, Phase: omev1beta1.OMENativeInstanceReady, ReadySince: &ready, Operation: &omev1beta1.InstanceOperation{Reason: "NONSELECTED_SENTINEL"}},
	}
	surge := int32(2)
	ir.Status.Migrations = []omev1beta1.MigrationStatus{
		{RequestUUID: "source", Trigger: omev1beta1.MigrationTriggerManual, SourceInstance: 2, Phase: omev1beta1.MigrationPhaseDraining, FromNode: "node-a", Reason: "move"},
		{RequestUUID: "surge", Trigger: omev1beta1.MigrationTriggerManual, SourceInstance: 1, SurgeInstance: &surge, Phase: omev1beta1.MigrationPhaseSurgeReady},
		{RequestUUID: "other", Trigger: omev1beta1.MigrationTriggerManual, SourceInstance: 9, Phase: omev1beta1.MigrationPhaseAccepted, Message: "NONSELECTED_SENTINEL"},
	}
	limits := collectionLimits()
	limits.Details = instancecollection.DetailLimits{MaxConditions: 4, MaxScannedConditions: 8, MaxNodeHints: 4, MaxScannedNodeHints: 8, MaxMigrations: 4, MaxScannedMigrations: 8, SelectedComponent: omev1beta1.EngineComponent, SelectedIndex: 2}
	got, err := instancecollection.CollectRelated(context.Background(), listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
		return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{ir}}, nil
	}), isvc, limits)
	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	require.Len(t, got.Items[0].Status.Migrations, 2)
	assert.Equal(t, []string{"source", "surge"}, []string{got.Items[0].Status.Migrations[0].RequestUUID, got.Items[0].Status.Migrations[1].RequestUUID})
	assert.Equal(t, int32(1), got.Items[0].Status.InstanceStatuses[0].ActiveOrdinal)
	require.NotNil(t, got.Items[0].Status.InstanceStatuses[0].ReadySince)
	assert.Nil(t, got.Items[0].Status.InstanceStatuses[1].ReadySince)
	encoded, err := json.Marshal(got.Items[0])
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "NONSELECTED_SENTINEL")
}

func TestCollectRelatedDropsMigrationDetailsBeyondScanBound(t *testing.T) {
	t.Parallel()
	isvc := collectionISVC()
	ir := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	ir.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{Index: 2, Phase: omev1beta1.OMENativeInstanceReady}}
	ir.Status.Migrations = make([]omev1beta1.MigrationStatus, 9)
	for i := range ir.Status.Migrations {
		ir.Status.Migrations[i] = omev1beta1.MigrationStatus{RequestUUID: "SECRET-MIGRATION", SourceInstance: 2}
	}
	limits := collectionLimits()
	limits.Details = instancecollection.DetailLimits{MaxConditions: 4, MaxScannedConditions: 8, MaxNodeHints: 4, MaxScannedNodeHints: 8, MaxMigrations: 4, MaxScannedMigrations: 8, SelectedComponent: omev1beta1.EngineComponent, SelectedIndex: 2}
	got, err := instancecollection.CollectRelated(context.Background(), listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
		return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{ir}}, nil
	}), isvc, limits)
	require.NoError(t, err)
	assert.Empty(t, got.Items[0].Status.Migrations)
	assert.Contains(t, got.DetailsTruncated, instancecollection.DetailTruncation{Name: "chat-engine", Component: omev1beta1.EngineComponent, Index: 2, Kind: instancecollection.DetailMigrations})
}

func TestCollectRelatedRejectsGlobalDuplicateMigrationIdentity(t *testing.T) {
	t.Parallel()
	isvc := collectionISVC()
	ir := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	ir.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{Index: 2, Phase: omev1beta1.OMENativeInstanceReady}}
	ir.Status.Migrations = []omev1beta1.MigrationStatus{
		{RequestUUID: "duplicate", SourceInstance: 2},
		{RequestUUID: "duplicate", SourceInstance: 9},
	}
	limits := collectionLimits()
	limits.Details = instancecollection.DetailLimits{MaxConditions: 4, MaxScannedConditions: 8, MaxNodeHints: 4, MaxScannedNodeHints: 8, MaxMigrations: 4, MaxScannedMigrations: 8, SelectedComponent: omev1beta1.EngineComponent, SelectedIndex: 2}

	got, err := instancecollection.CollectRelated(context.Background(), listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
		return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{ir}}, nil
	}), isvc, limits)

	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	assert.Empty(t, got.Items[0].Status.Migrations)
	assert.Contains(t, got.DetailsTruncated, instancecollection.DetailTruncation{Name: "chat-engine", Component: omev1beta1.EngineComponent, Index: 2, Kind: instancecollection.DetailMigrations})
}

func TestCollectRelatedBoundsAndDefensivelyCopiesRetryBlocks(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	large := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	large.Status.RetryBlocks = make([]omev1beta1.RetryBlock, 10_000)
	for index := range large.Status.RetryBlocks {
		large.Status.RetryBlocks[index] = omev1beta1.RetryBlock{
			TargetRevision: "chat-engine-oversized",
			State:          omev1beta1.RetryBlockHeld,
			Reason:         "SECRET-OVERSIZED-REASON",
		}
	}
	small := relatedReplica(isvc, "chat-decoder", omev1beta1.DecoderComponent)
	nextRetry := metav1.NewTime(time.Date(2026, 9, 14, 22, 0, 0, 0, time.UTC))
	small.Status.RetryBlocks = []omev1beta1.RetryBlock{{
		TargetRevision:  "chat-decoder-aaaaaaaa",
		State:           omev1beta1.RetryBlockBackoff,
		AttemptsStarted: 2,
		NextRetryAt:     &nextRetry,
		Reason:          "pull failed",
	}}
	limits := collectionLimits()
	limits.MaxRetryBlocks = 1
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
		"retry-block budget selection must not inherit API response order")
	require.Len(t, got.Items, 2)
	assert.Empty(t, got.Items[0].Status.RetryBlocks)
	require.Len(t, got.Items[1].Status.RetryBlocks, 1)
	assert.Equal(t, []instancecollection.RetryBlocksTruncation{{
		Name: "chat-engine", Component: omev1beta1.EngineComponent,
	}}, got.RetryBlocksTruncated)

	got.Items[1].Status.RetryBlocks[0].Reason = "changed"
	got.Items[1].Status.RetryBlocks[0].NextRetryAt.Time = time.Time{}
	assert.Equal(t, "pull failed", small.Status.RetryBlocks[0].Reason)
	assert.Equal(t, nextRetry.Time, small.Status.RetryBlocks[0].NextRetryAt.Time)
	encoded, marshalErr := json.Marshal(got.Items[0])
	require.NoError(t, marshalErr)
	assert.NotContains(t, string(encoded), "SECRET-OVERSIZED-REASON")
}

func TestCollectRelatedBoundsRetryBlockStringsByBytesAndUTF8(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	ir := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
		TargetRevision:  strings.Repeat("界", 400),
		State:           omev1beta1.RetryBlockHeld,
		AttemptsStarted: 1,
		Reason:          strings.Repeat("\xffa", 3_000),
	}}
	limits := collectionLimits()
	limits.MaxRetryBlocks = 1

	got, err := instancecollection.CollectRelated(
		context.Background(),
		listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
			return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{ir}}, nil
		}),
		isvc,
		limits,
	)

	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	require.Len(t, got.Items[0].Status.RetryBlocks, 1)
	block := got.Items[0].Status.RetryBlocks[0]
	assert.LessOrEqual(t, len(block.TargetRevision), 1_024)
	assert.LessOrEqual(t, len(block.Reason), 4_096)
	assert.True(t, utf8.ValidString(block.TargetRevision))
	assert.True(t, utf8.ValidString(block.Reason))
}

func TestCollectRelatedOmitsRetryBlocksWhenBudgetIsDisabled(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	ir := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
		TargetRevision: "chat-engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld,
	}}

	got, err := instancecollection.CollectRelated(
		context.Background(),
		listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
			return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{ir}}, nil
		}),
		isvc,
		collectionLimits(),
	)

	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	assert.Empty(t, got.Items[0].Status.RetryBlocks)
	assert.Empty(t, got.RetryBlocksTruncated)
}

func TestCollectRelatedOptionallyCopiesBoundedInstanceDetails(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	source := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	exitCode := int32(137)
	source.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{
		Index: 2, Incarnation: 7, Phase: omev1beta1.OMENativeInstanceUpdating,
		Conditions: []metav1.Condition{
			{Type: "Zeta", Status: metav1.ConditionFalse, Reason: "later", Message: "must-not-copy"},
			{Type: "AllPodsReady", Status: metav1.ConditionTrue, Reason: "Ready"},
			{Type: "Drained", Status: metav1.ConditionFalse, Reason: "Serving"},
		},
		Operation: &omev1beta1.InstanceOperation{
			ID: "op", Type: omev1beta1.InstanceOperationUpdate, Step: "WaitReady",
			HintTargetNodes: []string{"node-c", "node-a", "node-b"},
		},
		LastFailure: &omev1beta1.InstanceTermination{
			PodName: "pod", ContainerName: "runner", Reason: "OOMKilled", ExitCode: &exitCode,
			Message: "must-not-copy",
		},
	}}
	lister := listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
		return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{source}}, nil
	})
	limits := collectionLimits()
	limits.Details = instancecollection.DetailLimits{
		MaxConditions: 2, MaxScannedConditions: 4, MaxNodeHints: 2, MaxScannedNodeHints: 4,
		SelectedComponent: omev1beta1.EngineComponent, SelectedIndex: 2,
	}

	got, err := instancecollection.CollectRelated(context.Background(), lister, isvc, limits)
	require.NoError(t, err)
	require.Len(t, got.Items, 1)
	row := got.Items[0].Status.InstanceStatuses[0]
	require.Len(t, row.Conditions, 2)
	assert.Equal(t, []string{"AllPodsReady", "Drained"}, []string{row.Conditions[0].Type, row.Conditions[1].Type})
	assert.Empty(t, row.Conditions[0].Message)
	require.NotNil(t, row.Operation)
	assert.Equal(t, []string{"node-a", "node-b"}, row.Operation.HintTargetNodes)
	require.NotNil(t, row.LastFailure)
	assert.Empty(t, row.LastFailure.Message)
	assert.Equal(t, int32(137), *row.LastFailure.ExitCode)
	assert.Equal(t, []instancecollection.DetailTruncation{
		{Name: "chat-engine", Component: omev1beta1.EngineComponent, Index: 2, Kind: instancecollection.DetailConditions},
		{Name: "chat-engine", Component: omev1beta1.EngineComponent, Index: 2, Kind: instancecollection.DetailNodeHints},
	}, got.DetailsTruncated)

	source.Status.InstanceStatuses[0].Conditions[0].Reason = "mutated"
	source.Status.InstanceStatuses[0].Operation.HintTargetNodes[0] = "mutated"
	assert.Equal(t, "Ready", row.Conditions[0].Reason)
	assert.Equal(t, "node-a", row.Operation.HintTargetNodes[0])
}

func TestCollectRelatedCopiesDetailsOnlyForSelectedInstance(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	engine := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	engine.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: omev1beta1.OMENativeInstanceReady, RunningRevision: strings.Repeat("r", 10000), Conditions: []metav1.Condition{{Type: "NONSELECTED_SENTINEL"}}, Operation: &omev1beta1.InstanceOperation{Reason: "NONSELECTED_SENTINEL"}, LastFailure: &omev1beta1.InstanceTermination{Reason: "NONSELECTED_SENTINEL"}},
		{Index: 2, Phase: omev1beta1.OMENativeInstanceReady, Conditions: []metav1.Condition{{Type: "Selected", Status: metav1.ConditionTrue}}, Operation: &omev1beta1.InstanceOperation{ID: "op", Type: omev1beta1.InstanceOperationUpdate, Step: "step"}, LastFailure: &omev1beta1.InstanceTermination{PodName: "pod", Reason: "selected"}},
	}
	decoder := relatedReplica(isvc, "chat-decoder", omev1beta1.DecoderComponent)
	decoder.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{Index: 2, Conditions: []metav1.Condition{{Type: "NONSELECTED_SENTINEL"}}, Operation: &omev1beta1.InstanceOperation{Reason: "NONSELECTED_SENTINEL"}}}
	limits := collectionLimits()
	limits.Details = instancecollection.DetailLimits{
		MaxConditions: 2, MaxScannedConditions: 2, MaxNodeHints: 2, MaxScannedNodeHints: 2,
		SelectedComponent: omev1beta1.EngineComponent, SelectedIndex: 2,
	}
	got, err := instancecollection.CollectRelated(context.Background(), listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
		return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{decoder, engine}}, nil
	}), isvc, limits)
	require.NoError(t, err)
	encoded, err := json.Marshal(got.Items)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "NONSELECTED_SENTINEL")
	var engineCopy omev1beta1.InferenceReplica
	for _, item := range got.Items {
		if item.Spec.Component == omev1beta1.EngineComponent {
			engineCopy = item
		}
	}
	require.Len(t, engineCopy.Status.InstanceStatuses, 2)
	assert.Nil(t, engineCopy.Status.InstanceStatuses[0].Operation)
	assert.Nil(t, engineCopy.Status.InstanceStatuses[0].LastFailure)
	assert.Empty(t, engineCopy.Status.InstanceStatuses[0].Conditions)
	assert.LessOrEqual(t, len(engineCopy.Status.InstanceStatuses[0].RunningRevision), 1024)
	require.NotNil(t, engineCopy.Status.InstanceStatuses[1].Operation)
	require.NotNil(t, engineCopy.Status.InstanceStatuses[1].LastFailure)
	assert.Equal(t, "Selected", engineCopy.Status.InstanceStatuses[1].Conditions[0].Type)
}

func TestCollectRelatedDropsDetailsThatExceedScanBounds(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	source := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	source.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{
		Index: 0, Phase: omev1beta1.OMENativeInstanceReady,
		Conditions: []metav1.Condition{{Type: "A"}, {Type: "B"}, {Type: "SECRET"}},
		Operation:  &omev1beta1.InstanceOperation{HintTargetNodes: []string{"a", "b", "SECRET"}},
	}}
	limits := collectionLimits()
	limits.Details = instancecollection.DetailLimits{
		MaxConditions: 1, MaxScannedConditions: 2, MaxNodeHints: 1, MaxScannedNodeHints: 2,
		SelectedComponent: omev1beta1.EngineComponent, SelectedIndex: 0,
	}
	got, err := instancecollection.CollectRelated(context.Background(), listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
		return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{source}}, nil
	}), isvc, limits)
	require.NoError(t, err)
	row := got.Items[0].Status.InstanceStatuses[0]
	assert.Empty(t, row.Conditions)
	assert.Empty(t, row.Operation.HintTargetNodes)
	encoded, err := json.Marshal(got.Items)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "SECRET")
}

func TestCollectRelatedBoundsEveryCopiedDetailStringByBytes(t *testing.T) {
	t.Parallel()

	isvc := collectionISVC()
	source := relatedReplica(isvc, "chat-engine", omev1beta1.EngineComponent)
	oversized := strings.Repeat("界", 2000)
	source.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{{
		Index: 0, Phase: omev1beta1.OMENativeInstanceReady,
		Conditions: []metav1.Condition{{Type: oversized, Status: metav1.ConditionTrue, Reason: oversized}},
		Operation: &omev1beta1.InstanceOperation{
			ID: oversized, Type: omev1beta1.InstanceOperationMigrate, Step: oversized,
			TargetRevision: oversized, Reason: oversized, FromNode: oversized,
			HintTargetNodes: []string{oversized}, RequestUUID: oversized,
		},
		LastFailure: &omev1beta1.InstanceTermination{
			PodName: oversized, ContainerName: oversized, Reason: oversized, Message: "must-not-copy",
		},
	}}
	limits := collectionLimits()
	limits.Details = instancecollection.DetailLimits{
		MaxConditions: 2, MaxScannedConditions: 2, MaxNodeHints: 2, MaxScannedNodeHints: 2,
		SelectedComponent: omev1beta1.EngineComponent, SelectedIndex: 0,
	}
	got, err := instancecollection.CollectRelated(context.Background(), listerFunc(func(context.Context, metav1.ListOptions) (*omev1beta1.InferenceReplicaList, error) {
		return &omev1beta1.InferenceReplicaList{Items: []omev1beta1.InferenceReplica{source}}, nil
	}), isvc, limits)
	require.NoError(t, err)
	row := got.Items[0].Status.InstanceStatuses[0]
	values := []string{
		row.Conditions[0].Type, row.Conditions[0].Reason,
		row.Operation.ID, row.Operation.Step, row.Operation.TargetRevision, row.Operation.Reason,
		row.Operation.FromNode, row.Operation.HintTargetNodes[0], row.Operation.RequestUUID,
		row.LastFailure.PodName, row.LastFailure.ContainerName, row.LastFailure.Reason,
	}
	for _, value := range values {
		assert.LessOrEqual(t, len(value), 1024)
		assert.True(t, utf8.ValidString(value))
	}
	assert.Empty(t, row.LastFailure.Message)
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
		{name: "invalid retry block limit", lister: panicLister{}, isvc: isvc, limits: instancecollection.Limits{Paging: collectionLimits().Paging, MaxStatusRows: 1, MaxRetryBlocks: -1}, want: instancecollection.ErrMaxRetryBlocksInvalid},
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
