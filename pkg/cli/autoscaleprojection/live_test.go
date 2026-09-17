package autoscaleprojection_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/autoscaleprojection"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
)

type scaleRead struct {
	namespace, name string
}

type fakeScaleReader struct {
	scale   *autoscalingv1.Scale
	ir      *ome.InferenceReplica
	err     error
	irErr   error
	reads   []scaleRead
	onIRGet func()
}

func (r *fakeScaleReader) GetInferenceReplicaScale(_ context.Context, namespace, name string, _ metav1.GetOptions) (*autoscalingv1.Scale, error) {
	r.reads = append(r.reads, scaleRead{namespace, name})
	return r.scale, r.err
}

func (r *fakeScaleReader) GetInferenceReplica(_ context.Context, namespace, name string, _ metav1.GetOptions) (*ome.InferenceReplica, error) {
	r.reads = append(r.reads, scaleRead{namespace, name})
	if r.onIRGet != nil {
		r.onIRGet()
	}
	if r.ir != nil || r.irErr != nil {
		return r.ir, r.irErr
	}
	return liveReplica(), nil
}

func liveReplica() *ome.InferenceReplica {
	controller := true
	replicas := int32(3)
	return &ome.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: "chat-engine", Namespace: "prod", UID: types.UID("ir-uid"), ResourceVersion: "9", Generation: 2,
		Annotations:     map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: "3"},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "chat", UID: types.UID("parent-uid"), Controller: &controller}}},
		Spec:   ome.InferenceReplicaSpec{ParentRef: ome.ParentReference{Name: "chat"}, Component: ome.EngineComponent, Replicas: &replicas},
		Status: ome.InferenceReplicaStatus{ObservedGeneration: 2, Replicas: 2}}
}

func liveParent() *ome.InferenceService {
	transition := metav1.NewTime(time.Date(2026, 9, 17, 22, 0, 0, 0, time.UTC))
	return &ome.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: types.UID("parent-uid"), Generation: 3},
		Status: ome.InferenceServiceStatus{Components: map[ome.ComponentType]ome.ComponentStatusSpec{
			ome.EngineComponent: {ScaleTargetRef: &ome.ScaleTargetRef{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "chat-engine"},
				Autoscaler: &ome.ComponentAutoscalerStatus{Class: ome.AutoscalerHPA, ManagedBy: ome.AutoscalerManagedByOME,
					SpecSource: "default", CurrentReplicas: 2, DesiredReplicas: 3,
					Conditions: []metav1.Condition{{Type: "AbleToScale", Status: metav1.ConditionTrue, Reason: "Observed", LastTransitionTime: transition},
						{Type: "ScalingActive", Status: metav1.ConditionTrue, Reason: "Observed", LastTransitionTime: transition}}}},
		}}}
}

func liveScale(spec, current int32) *autoscalingv1.Scale {
	return &autoscalingv1.Scale{TypeMeta: metav1.TypeMeta{APIVersion: "autoscaling/v1", Kind: "Scale"},
		ObjectMeta: metav1.ObjectMeta{Name: "chat-engine", Namespace: "prod", UID: types.UID("ir-uid"), ResourceVersion: "9"},
		Spec:       autoscalingv1.ScaleSpec{Replicas: spec}, Status: autoscalingv1.ScaleStatus{Replicas: current}}
}

func TestLiveScaleReadsOnlyParentSelectedIRAndComparesCounts(t *testing.T) {
	parent := liveParent()
	base, err := autoscaleprojection.Project(parent, nil)
	require.NoError(t, err)
	require.Equal(t, reportv1alpha1.AutoscaleStateReported, base.Content.Summary.State)
	require.Empty(t, base.Warnings)
	reader := &fakeScaleReader{scale: liveScale(3, 2)}
	observedAt := time.Date(2026, 9, 17, 23, 0, 0, 0, time.UTC)
	result, err := autoscaleprojection.EnrichLiveScale(context.Background(), parent, base, reader, reportv1alpha1.ClockFunc(func() time.Time { return observedAt }))
	require.NoError(t, err)
	require.Equal(t, []scaleRead{{"prod", "chat-engine"}, {"prod", "chat-engine"}}, reader.reads)
	require.Len(t, result.Content.Components, 1)
	live := result.Content.Components[0].LiveScale
	require.NotNil(t, live)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScaleReported, live.Evidence)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScaleEqual, live.CountComparison)
	require.Equal(t, int32(3), *live.SpecReplicas)
	require.Equal(t, int32(2), *live.CurrentReplicas)
	require.Empty(t, result.Warnings)
	require.Len(t, result.Sources, 3)
	require.Equal(t, reportv1alpha1.AutoscaleSourceInferenceReplica, result.Sources[1].Kind)
	require.Equal(t, reportv1alpha1.AutoscaleSourceInferenceReplicaScale, result.Sources[2].Kind)
	for _, source := range result.Sources[1:] {
		require.Equal(t, "prod", source.Namespace)
		require.Equal(t, "chat-engine", source.Name)
		require.Equal(t, "ir-uid", source.UID)
		require.Equal(t, reportv1alpha1.EvidenceObserved, source.Evidence)
		require.Equal(t, observedAt, source.CollectedAt)
	}
}

func TestLiveScaleUnavailableAddsWarningWithoutChangingParentSummary(t *testing.T) {
	parent := liveParent()
	base, err := autoscaleprojection.Project(parent, nil)
	require.NoError(t, err)
	reader := &fakeScaleReader{irErr: apierrors.NewForbidden(schema.GroupResource{Group: "ome.io", Resource: "inferencereplicas"}, "chat-engine", errors.New("secret"))}
	result, err := autoscaleprojection.EnrichLiveScale(context.Background(), parent, base, reader, nil)
	require.NoError(t, err)
	require.Equal(t, reportv1alpha1.AutoscaleStateReported, result.Content.Summary.State)
	require.Equal(t, []reportv1alpha1.AutoscaleWarning{{Code: reportv1alpha1.AutoscaleWarningPartialData}}, result.Warnings)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScaleForbidden, result.Content.Components[0].LiveScale.Evidence)
	require.Len(t, result.Sources, 2)
	require.Equal(t, reportv1alpha1.EvidenceUnavailable, result.Sources[1].Evidence)
}

func TestLiveScaleDriftAndUnreadableAreExplicit(t *testing.T) {
	parent := liveParent()
	base, err := autoscaleprojection.Project(parent, nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		name       string
		scale      *autoscalingv1.Scale
		err        error
		evidence   reportv1alpha1.AutoscaleLiveScaleEvidence
		comparison reportv1alpha1.AutoscaleLiveScaleComparison
		hasCounts  bool
	}{
		{"drift", liveScale(4, 2), nil, reportv1alpha1.AutoscaleLiveScaleReported, reportv1alpha1.AutoscaleLiveScaleDrift, true},
		{"forbidden", nil, apierrors.NewForbidden(schema.GroupResource{Group: "ome.io", Resource: "inferencereplicas"}, "chat-engine", errors.New("private token")), reportv1alpha1.AutoscaleLiveScaleForbidden, reportv1alpha1.AutoscaleLiveScaleUnknown, false},
		{"not found", nil, apierrors.NewNotFound(schema.GroupResource{Group: "ome.io", Resource: "inferencereplicas"}, "chat-engine"), reportv1alpha1.AutoscaleLiveScaleNotFound, reportv1alpha1.AutoscaleLiveScaleUnknown, false},
		{"unavailable", nil, errors.New("private token"), reportv1alpha1.AutoscaleLiveScaleUnavailable, reportv1alpha1.AutoscaleLiveScaleUnknown, false},
		{"wrong name", func() *autoscalingv1.Scale { v := liveScale(3, 2); v.Name = "other"; return v }(), nil, reportv1alpha1.AutoscaleLiveScaleInvalid, reportv1alpha1.AutoscaleLiveScaleUnknown, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ir := liveReplica()
			if tc.name == "drift" {
				*ir.Spec.Replicas = 4
			}
			reader := &fakeScaleReader{ir: ir, scale: tc.scale, err: tc.err}
			result, err := autoscaleprojection.EnrichLiveScale(context.Background(), parent, base, reader, nil)
			require.NoError(t, err)
			live := result.Content.Components[0].LiveScale
			require.NotNil(t, live)
			require.Equal(t, tc.evidence, live.Evidence)
			require.Equal(t, tc.comparison, live.CountComparison)
			require.Equal(t, tc.hasCounts, live.SpecReplicas != nil)
			require.Equal(t, tc.hasCounts, live.CurrentReplicas != nil)
		})
	}
}

func TestLiveScaleDetectsDeletingIRAbsentFromCRDScaleMetadata(t *testing.T) {
	parent := liveParent()
	base, err := autoscaleprojection.Project(parent, nil)
	require.NoError(t, err)
	ir := liveReplica()
	now := metav1.Now()
	ir.DeletionTimestamp = &now
	reader := &fakeScaleReader{ir: ir, scale: liveScale(3, 2)}
	result, err := autoscaleprojection.EnrichLiveScale(context.Background(), parent, base, reader, nil)
	require.NoError(t, err)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScaleDeleting, result.Content.Components[0].LiveScale.Evidence)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScaleUnknown, result.Content.Components[0].LiveScale.CountComparison)
	require.Nil(t, result.Content.Components[0].LiveScale.SpecReplicas)
}

func TestLiveScaleStaleIRObservedGenerationStopsBeforeScale(t *testing.T) {
	parent := liveParent()
	base, err := autoscaleprojection.Project(parent, nil)
	require.NoError(t, err)
	ir := liveReplica()
	ir.Status.ObservedGeneration = ir.Generation - 1
	reader := &fakeScaleReader{ir: ir, scale: liveScale(3, 2)}
	result, err := autoscaleprojection.EnrichLiveScale(context.Background(), parent, base, reader, nil)
	require.NoError(t, err)
	require.Equal(t, []scaleRead{{"prod", "chat-engine"}}, reader.reads)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScaleStale, result.Content.Components[0].LiveScale.Evidence)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScaleUnknown, result.Content.Components[0].LiveScale.CountComparison)
	require.Len(t, result.Sources, 2)
	require.Equal(t, reportv1alpha1.EvidenceObserved, result.Sources[1].Evidence)
	require.Equal(t, []reportv1alpha1.AutoscaleWarning{{Code: reportv1alpha1.AutoscaleWarningPartialData}}, result.Warnings)
}

func TestLiveScaleCancellationAfterIRReadStopsBeforeScale(t *testing.T) {
	parent := liveParent()
	base, err := autoscaleprojection.Project(parent, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &fakeScaleReader{ir: liveReplica(), scale: liveScale(3, 2), onIRGet: cancel}
	result, err := autoscaleprojection.EnrichLiveScale(ctx, parent, base, reader, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, reportv1alpha1.AutoscaleStatusReport{}, result)
	require.Equal(t, []scaleRead{{"prod", "chat-engine"}}, reader.reads)
}

func TestLiveScaleRefusesForeignOrChangedIRIdentity(t *testing.T) {
	parent := liveParent()
	base, err := autoscaleprojection.Project(parent, nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		change func(*fakeScaleReader)
		want   reportv1alpha1.AutoscaleLiveScaleEvidence
	}{
		{"foreign owner", func(r *fakeScaleReader) { r.ir.OwnerReferences[0].UID = "other" }, reportv1alpha1.AutoscaleLiveScaleInvalid},
		{"stale parent generation", func(r *fakeScaleReader) {
			r.ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "2"
		}, reportv1alpha1.AutoscaleLiveScaleInvalid},
		{"changed scale UID", func(r *fakeScaleReader) { r.scale.UID = "replacement" }, reportv1alpha1.AutoscaleLiveScaleChanged},
		{"changed scale resource version", func(r *fakeScaleReader) { r.scale.ResourceVersion = "10" }, reportv1alpha1.AutoscaleLiveScaleChanged},
		{"changed counts at same resource version", func(r *fakeScaleReader) { r.scale.Spec.Replicas = 4 }, reportv1alpha1.AutoscaleLiveScaleChanged},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &fakeScaleReader{ir: liveReplica(), scale: liveScale(3, 2)}
			tc.change(reader)
			result, err := autoscaleprojection.EnrichLiveScale(context.Background(), parent, base, reader, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, result.Content.Components[0].LiveScale.Evidence)
			require.Equal(t, reportv1alpha1.AutoscaleLiveScaleUnknown, result.Content.Components[0].LiveScale.CountComparison)
		})
	}
}

func TestLiveScaleSkipsUnsupportedOrMissingTarget(t *testing.T) {
	for _, tc := range []struct {
		name string
		ref  *ome.ScaleTargetRef
		want reportv1alpha1.AutoscaleLiveScaleEvidence
	}{
		{"deployment", &ome.ScaleTargetRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "chat-engine"}, reportv1alpha1.AutoscaleLiveScaleUnsupported},
		{"missing", nil, reportv1alpha1.AutoscaleLiveScaleNotSelected},
		{"malformed", &ome.ScaleTargetRef{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "Bad_Name"}, reportv1alpha1.AutoscaleLiveScaleInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := liveParent()
			status := parent.Status.Components[ome.EngineComponent]
			status.ScaleTargetRef = tc.ref
			parent.Status.Components[ome.EngineComponent] = status
			base, err := autoscaleprojection.Project(parent, nil)
			require.NoError(t, err)
			reader := &fakeScaleReader{scale: liveScale(3, 2)}
			result, err := autoscaleprojection.EnrichLiveScale(context.Background(), parent, base, reader, nil)
			require.NoError(t, err)
			require.Empty(t, reader.reads)
			require.Equal(t, tc.want, result.Content.Components[0].LiveScale.Evidence)
		})
	}
}
