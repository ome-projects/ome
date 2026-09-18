package autoscaleprojection_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/stretchr/testify/require"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/autoscaleprojection"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

type scalerRead struct{ kind, namespace, name string }

type fakeScalerReader struct {
	ir     *ome.InferenceReplica
	hpa    *autoscalingv2.HorizontalPodAutoscaler
	so     *kedav1.ScaledObject
	irErr  error
	hpaErr error
	soErr  error
	reads  []scalerRead
}

func (r *fakeScalerReader) GetInferenceReplica(_ context.Context, namespace, name string, _ metav1.GetOptions) (*ome.InferenceReplica, error) {
	r.reads = append(r.reads, scalerRead{"IR", namespace, name})
	return r.ir, r.irErr
}

func (r *fakeScalerReader) GetHPA(_ context.Context, namespace, name string, _ metav1.GetOptions) (*autoscalingv2.HorizontalPodAutoscaler, error) {
	r.reads = append(r.reads, scalerRead{"HPA", namespace, name})
	return r.hpa, r.hpaErr
}

func (r *fakeScalerReader) GetScaledObject(_ context.Context, namespace, name string, _ metav1.GetOptions) (*kedav1.ScaledObject, error) {
	r.reads = append(r.reads, scalerRead{"SO", namespace, name})
	return r.so, r.soErr
}

func testScalerHPA() *autoscalingv2.HorizontalPodAutoscaler {
	controller := true
	observed := int64(4)
	return &autoscalingv2.HorizontalPodAutoscaler{
		TypeMeta: metav1.TypeMeta{APIVersion: "autoscaling/v2", Kind: "HorizontalPodAutoscaler"},
		ObjectMeta: metav1.ObjectMeta{Name: "chat-engine", Namespace: "prod", UID: types.UID("hpa-uid"), Generation: 4,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "chat-engine", UID: types.UID("ir-uid"), Controller: &controller}}},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "chat-engine"}, MaxReplicas: 8},
		Status: autoscalingv2.HorizontalPodAutoscalerStatus{ObservedGeneration: &observed, CurrentReplicas: 2, DesiredReplicas: 3,
			Conditions: []autoscalingv2.HorizontalPodAutoscalerCondition{{Type: autoscalingv2.ScalingActive, Status: "True", Reason: "private", Message: "secret-token"}}},
	}
}

func testScalerSO() *kedav1.ScaledObject {
	controller := true
	return &kedav1.ScaledObject{
		TypeMeta: metav1.TypeMeta{APIVersion: "keda.sh/v1alpha1", Kind: "ScaledObject"},
		ObjectMeta: metav1.ObjectMeta{Name: "scaledobject-chat-engine", Namespace: "prod", UID: types.UID("so-uid"), Generation: 4,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "chat-engine", UID: types.UID("ir-uid"), Controller: &controller}}},
		Spec:   kedav1.ScaledObjectSpec{ScaleTargetRef: &kedav1.ScaleTarget{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "chat-engine"}},
		Status: kedav1.ScaledObjectStatus{HpaName: "generated-private-hpa", Conditions: kedav1.Conditions{{Type: kedav1.ConditionReady, Status: "True", Reason: "private", Message: "secret-token"}}},
	}
}

func TestLiveScalerHPAReadsOnlySelectedObjectsAndRedactsMessages(t *testing.T) {
	parent := liveParent()
	base, err := autoscaleprojection.Project(parent, nil)
	require.NoError(t, err)
	reader := &fakeScalerReader{ir: liveReplica(), hpa: testScalerHPA()}
	got, err := autoscaleprojection.EnrichLiveScaler(context.Background(), parent, base, reader, nil)
	require.NoError(t, err)
	require.Equal(t, []scalerRead{{"IR", "prod", "chat-engine"}, {"HPA", "prod", "chat-engine"}}, reader.reads)
	live := got.Content.Components[0].LiveScaler
	require.NotNil(t, live)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScalerReported, live.Evidence)
	require.Equal(t, reportv1alpha1.AutoscaleClassHPA, live.Kind)
	require.Equal(t, reportv1alpha1.AutoscaleScalerGenerationMatched, live.GenerationState)
	require.Equal(t, int32(2), *live.CurrentReplicas)
	require.Equal(t, int32(3), *live.DesiredReplicas)
	require.Equal(t, []reportv1alpha1.AutoscaleScalerCondition{{Type: reportv1alpha1.AutoscaleConditionScalingActive, Status: reportv1alpha1.AutoscaleConditionTrue}}, live.Conditions)
	raw, err := json.Marshal(got)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "secret-token")
	require.NotContains(t, string(raw), "private")
}

func TestLiveScalerKEDAReadsOnlyScaledObjectAndDoesNotInferDerivedHPA(t *testing.T) {
	parent := liveParent()
	status := parent.Status.Components[ome.EngineComponent]
	status.Autoscaler.Class = ome.AutoscalerKEDA
	parent.Status.Components[ome.EngineComponent] = status
	base, err := autoscaleprojection.Project(parent, nil)
	require.NoError(t, err)
	reader := &fakeScalerReader{ir: liveReplica(), so: testScalerSO()}
	got, err := autoscaleprojection.EnrichLiveScaler(context.Background(), parent, base, reader, nil)
	require.NoError(t, err)
	require.Equal(t, []scalerRead{{"IR", "prod", "chat-engine"}, {"SO", "prod", "scaledobject-chat-engine"}}, reader.reads)
	live := got.Content.Components[0].LiveScaler
	require.Equal(t, reportv1alpha1.AutoscaleLiveScalerReported, live.Evidence)
	require.Equal(t, reportv1alpha1.AutoscaleClassKEDA, live.Kind)
	require.Equal(t, reportv1alpha1.AutoscaleScalerGenerationUnproven, live.GenerationState)
	require.Nil(t, live.CurrentReplicas)
	require.Nil(t, live.DesiredReplicas)
	require.Equal(t, []reportv1alpha1.AutoscaleScalerCondition{{Type: reportv1alpha1.AutoscaleConditionReady, Status: reportv1alpha1.AutoscaleConditionTrue}}, live.Conditions)
	raw, err := json.Marshal(got)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "generated-private-hpa")
	require.NotContains(t, string(raw), "secret-token")
}

func TestLiveScalerRejectsForeignIRBeforeScalerRead(t *testing.T) {
	parent := liveParent()
	base, err := autoscaleprojection.Project(parent, nil)
	require.NoError(t, err)
	ir := liveReplica()
	ir.OwnerReferences[0].UID = "foreign"
	reader := &fakeScalerReader{ir: ir, hpa: testScalerHPA()}
	got, err := autoscaleprojection.EnrichLiveScaler(context.Background(), parent, base, reader, nil)
	require.NoError(t, err)
	require.Equal(t, []scalerRead{{"IR", "prod", "chat-engine"}}, reader.reads)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScalerInvalid, got.Content.Components[0].LiveScaler.Evidence)
	require.Nil(t, got.Content.Components[0].LiveScaler.CurrentReplicas)
}

func TestLiveScalerHPARejectsForeignTargetAndStaleGeneration(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*autoscalingv2.HorizontalPodAutoscaler)
		want   reportv1alpha1.AutoscaleLiveScalerEvidence
	}{
		{"foreign owner", func(h *autoscalingv2.HorizontalPodAutoscaler) { h.OwnerReferences[0].UID = "foreign" }, reportv1alpha1.AutoscaleLiveScalerInvalid},
		{"wrong target", func(h *autoscalingv2.HorizontalPodAutoscaler) { h.Spec.ScaleTargetRef.Name = "other" }, reportv1alpha1.AutoscaleLiveScalerInvalid},
		{"wrong version", func(h *autoscalingv2.HorizontalPodAutoscaler) { h.APIVersion = "autoscaling/v1" }, reportv1alpha1.AutoscaleLiveScalerInvalid},
		{"stale generation", func(h *autoscalingv2.HorizontalPodAutoscaler) { *h.Status.ObservedGeneration = 3 }, reportv1alpha1.AutoscaleLiveScalerStale},
		{"negative counts", func(h *autoscalingv2.HorizontalPodAutoscaler) { h.Status.CurrentReplicas = -1 }, reportv1alpha1.AutoscaleLiveScalerInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := liveParent()
			base, err := autoscaleprojection.Project(parent, nil)
			require.NoError(t, err)
			hpa := testScalerHPA()
			tc.mutate(hpa)
			got, err := autoscaleprojection.EnrichLiveScaler(context.Background(), parent, base, &fakeScalerReader{ir: liveReplica(), hpa: hpa}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Content.Components[0].LiveScaler.Evidence)
			require.Nil(t, got.Content.Components[0].LiveScaler.CurrentReplicas)
			require.Empty(t, got.Content.Components[0].LiveScaler.Conditions)
		})
	}
}

func TestLiveScalerClassifiesReadFailureWithoutErrorText(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want reportv1alpha1.AutoscaleLiveScalerEvidence
	}{
		{"forbidden", apierrors.NewForbidden(schema.GroupResource{Group: "autoscaling", Resource: "horizontalpodautoscalers"}, "chat-engine", errors.New("secret-token")), reportv1alpha1.AutoscaleLiveScalerForbidden},
		{"not found", apierrors.NewNotFound(schema.GroupResource{Group: "autoscaling", Resource: "horizontalpodautoscalers"}, "chat-engine"), reportv1alpha1.AutoscaleLiveScalerNotFound},
		{"unavailable", errors.New("secret-token"), reportv1alpha1.AutoscaleLiveScalerUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := liveParent()
			base, err := autoscaleprojection.Project(parent, nil)
			require.NoError(t, err)
			got, err := autoscaleprojection.EnrichLiveScaler(context.Background(), parent, base, &fakeScalerReader{ir: liveReplica(), hpaErr: tc.err}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Content.Components[0].LiveScaler.Evidence)
			raw, err := json.Marshal(got)
			require.NoError(t, err)
			require.NotContains(t, string(raw), "secret-token")
		})
	}
}

func TestLiveScalerSkipsExternalOwnerAndUnsupportedTargetsWithoutReads(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*ome.InferenceService)
		want   reportv1alpha1.AutoscaleLiveScalerEvidence
	}{
		{"external", func(p *ome.InferenceService) {
			s := p.Status.Components[ome.EngineComponent]
			s.Autoscaler.Class = ome.AutoscalerExternal
			s.Autoscaler.ManagedBy = ome.AutoscalerManagedByExternal
			s.Autoscaler.CurrentReplicas = 0
			s.Autoscaler.DesiredReplicas = 0
			s.Autoscaler.Conditions = nil
			p.Status.Components[ome.EngineComponent] = s
		}, reportv1alpha1.AutoscaleLiveScalerNotSelected},
		{"missing", func(p *ome.InferenceService) {
			s := p.Status.Components[ome.EngineComponent]
			s.ScaleTargetRef = nil
			p.Status.Components[ome.EngineComponent] = s
		}, reportv1alpha1.AutoscaleLiveScalerNotSelected},
		{"unsupported", func(p *ome.InferenceService) {
			s := p.Status.Components[ome.EngineComponent]
			s.ScaleTargetRef = &ome.ScaleTargetRef{APIVersion: "future.io/v1", Kind: "Future", Name: "chat-engine"}
			p.Status.Components[ome.EngineComponent] = s
		}, reportv1alpha1.AutoscaleLiveScalerUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := liveParent()
			tc.change(parent)
			base, err := autoscaleprojection.Project(parent, nil)
			require.NoError(t, err)
			reader := &fakeScalerReader{ir: liveReplica(), hpa: testScalerHPA()}
			got, err := autoscaleprojection.EnrichLiveScaler(context.Background(), parent, base, reader, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Content.Components[0].LiveScaler.Evidence)
			require.Empty(t, reader.reads)
			if tc.name == "external" {
				require.Empty(t, got.Warnings)
			}
		})
	}
}

func TestLiveScalerRejectsMalformedOrConflictingKnownConditions(t *testing.T) {
	parent := liveParent()
	base, err := autoscaleprojection.Project(parent, nil)
	require.NoError(t, err)
	hpa := testScalerHPA()
	hpa.Status.Conditions[0].Status = "Maybe"
	got, err := autoscaleprojection.EnrichLiveScaler(context.Background(), parent, base, &fakeScalerReader{ir: liveReplica(), hpa: hpa}, nil)
	require.NoError(t, err)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScalerInvalid, got.Content.Components[0].LiveScaler.Evidence)
	require.Nil(t, got.Content.Components[0].LiveScaler.CurrentReplicas)

	status := parent.Status.Components[ome.EngineComponent]
	status.Autoscaler.Class = ome.AutoscalerKEDA
	parent.Status.Components[ome.EngineComponent] = status
	base, err = autoscaleprojection.Project(parent, nil)
	require.NoError(t, err)
	so := testScalerSO()
	so.Status.Conditions = append(so.Status.Conditions, kedav1.Condition{Type: kedav1.ConditionReady, Status: "False"})
	got, err = autoscaleprojection.EnrichLiveScaler(context.Background(), parent, base, &fakeScalerReader{ir: liveReplica(), so: so}, nil)
	require.NoError(t, err)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScalerInvalid, got.Content.Components[0].LiveScaler.Evidence)
	require.Empty(t, got.Content.Components[0].LiveScaler.Conditions)
}

func TestLiveScalerRawDeploymentUsesParentOwnerWithoutIRRead(t *testing.T) {
	parent := liveParent()
	status := parent.Status.Components[ome.EngineComponent]
	status.ScaleTargetRef = &ome.ScaleTargetRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "chat-engine"}
	parent.Status.Components[ome.EngineComponent] = status
	base, err := autoscaleprojection.Project(parent, nil)
	require.NoError(t, err)
	hpa := testScalerHPA()
	hpa.Spec.ScaleTargetRef = autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "chat-engine"}
	hpa.OwnerReferences[0] = metav1.OwnerReference{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "chat", UID: "parent-uid", Controller: hpa.OwnerReferences[0].Controller}
	reader := &fakeScalerReader{hpa: hpa}
	got, err := autoscaleprojection.EnrichLiveScaler(context.Background(), parent, base, reader, nil)
	require.NoError(t, err)
	require.Equal(t, []scalerRead{{"HPA", "prod", "chat-engine"}}, reader.reads)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScalerReported, got.Content.Components[0].LiveScaler.Evidence)

	hpa.OwnerReferences[0].UID = "foreign-parent"
	got, err = autoscaleprojection.EnrichLiveScaler(context.Background(), parent, base, reader, nil)
	require.NoError(t, err)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScalerInvalid, got.Content.Components[0].LiveScaler.Evidence)
	require.Nil(t, got.Content.Components[0].LiveScaler.CurrentReplicas)
}

func TestLiveScalerKEDANoConditionsDoesNotClaimHealth(t *testing.T) {
	parent := liveParent()
	status := parent.Status.Components[ome.EngineComponent]
	status.Autoscaler.Class = ome.AutoscalerKEDA
	parent.Status.Components[ome.EngineComponent] = status
	base, err := autoscaleprojection.Project(parent, nil)
	require.NoError(t, err)
	so := testScalerSO()
	so.Status.Conditions = nil
	got, err := autoscaleprojection.EnrichLiveScaler(context.Background(), parent, base, &fakeScalerReader{ir: liveReplica(), so: so}, nil)
	require.NoError(t, err)
	live := got.Content.Components[0].LiveScaler
	require.Equal(t, reportv1alpha1.AutoscaleLiveScalerReported, live.Evidence)
	require.Equal(t, reportv1alpha1.AutoscaleScalerGenerationUnproven, live.GenerationState)
	require.Empty(t, live.Conditions)
	require.Nil(t, live.CurrentReplicas)
}

func TestLiveScalerMixedManagedAndExternalDoesNotWarnForExpectedSkip(t *testing.T) {
	parent := liveParent()
	parent.Status.Components[ome.DecoderComponent] = ome.ComponentStatusSpec{
		ScaleTargetRef: &ome.ScaleTargetRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "chat-decoder"},
		Autoscaler:     &ome.ComponentAutoscalerStatus{Class: ome.AutoscalerExternal, ManagedBy: ome.AutoscalerManagedByExternal, SpecSource: "isvc"},
	}
	base, err := autoscaleprojection.Project(parent, nil)
	require.NoError(t, err)
	require.Empty(t, base.Warnings)
	reader := &fakeScalerReader{ir: liveReplica(), hpa: testScalerHPA()}
	got, err := autoscaleprojection.EnrichLiveScaler(context.Background(), parent, base, reader, nil)
	require.NoError(t, err)
	require.Equal(t, []scalerRead{{"IR", "prod", "chat-engine"}, {"HPA", "prod", "chat-engine"}}, reader.reads)
	require.Empty(t, got.Warnings)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScalerReported, got.Content.Components[0].LiveScaler.Evidence)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScalerNotSelected, got.Content.Components[1].LiveScaler.Evidence)
}

func TestLiveScalerKEDARejectsForeignTargetAndDeletingObject(t *testing.T) {
	parent := liveParent()
	status := parent.Status.Components[ome.EngineComponent]
	status.Autoscaler.Class = ome.AutoscalerKEDA
	parent.Status.Components[ome.EngineComponent] = status
	base, err := autoscaleprojection.Project(parent, nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		mutate func(*kedav1.ScaledObject)
		want   reportv1alpha1.AutoscaleLiveScalerEvidence
	}{
		{"foreign owner", func(so *kedav1.ScaledObject) { so.OwnerReferences[0].UID = "foreign" }, reportv1alpha1.AutoscaleLiveScalerInvalid},
		{"wrong target", func(so *kedav1.ScaledObject) { so.Spec.ScaleTargetRef.Name = "other" }, reportv1alpha1.AutoscaleLiveScalerInvalid},
		{"deleting", func(so *kedav1.ScaledObject) { now := metav1.Now(); so.DeletionTimestamp = &now }, reportv1alpha1.AutoscaleLiveScalerDeleting},
	} {
		t.Run(tc.name, func(t *testing.T) {
			so := testScalerSO()
			tc.mutate(so)
			got, err := autoscaleprojection.EnrichLiveScaler(context.Background(), parent, base, &fakeScalerReader{ir: liveReplica(), so: so}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Content.Components[0].LiveScaler.Evidence)
			require.Empty(t, got.Content.Components[0].LiveScaler.Conditions)
		})
	}
}

func TestLiveScalerHPAWithoutObservedGenerationStaysUnprovenAndDeletingStopsEvidence(t *testing.T) {
	parent := liveParent()
	base, err := autoscaleprojection.Project(parent, nil)
	require.NoError(t, err)
	hpa := testScalerHPA()
	hpa.Status.ObservedGeneration = nil
	got, err := autoscaleprojection.EnrichLiveScaler(context.Background(), parent, base, &fakeScalerReader{ir: liveReplica(), hpa: hpa}, nil)
	require.NoError(t, err)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScalerReported, got.Content.Components[0].LiveScaler.Evidence)
	require.Equal(t, reportv1alpha1.AutoscaleScalerGenerationUnproven, got.Content.Components[0].LiveScaler.GenerationState)

	now := metav1.Now()
	hpa.DeletionTimestamp = &now
	got, err = autoscaleprojection.EnrichLiveScaler(context.Background(), parent, base, &fakeScalerReader{ir: liveReplica(), hpa: hpa}, nil)
	require.NoError(t, err)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScalerDeleting, got.Content.Components[0].LiveScaler.Evidence)
	require.Nil(t, got.Content.Components[0].LiveScaler.CurrentReplicas)
}
