package components

import (
	"context"
	"strings"
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/render"
)

// storedIRBlock is a typed-KEDA block standing in for the IR's persisted
// last-known-good render.
func storedIRBlock() *v1beta1.ComponentAutoscaler {
	return &v1beta1.ComponentAutoscaler{
		Class: v1beta1.AutoscalerKEDA,
		Keda: &v1beta1.KedaAutoscaler{
			Triggers: []kedav1.ScaleTriggers{{
				Type:     "prometheus",
				Metadata: map[string]string{"query": "requests_in_flight", "threshold": "5", "serverAddress": "http://prometheus.example.com:9090"},
			}},
		},
	}
}

func holdTestIR(name string, block *v1beta1.ComponentAutoscaler) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name + "-uid")},
		Spec:       v1beta1.InferenceReplicaSpec{Autoscaler: block},
	}
}

// TestDispatchIRAutoscaler_HoldRedispatchesStoredBlock pins the IR-managed
// hold: the ScaledObject is gone (deleted externally) but the IR still
// carries the stored last-known-good block, so a hold pass re-creates the
// SO from ir.Spec.Autoscaler — trigger content frozen, bounds still
// flowing from the component spec.
func TestDispatchIRAutoscaler_HoldRedispatchesStoredBlock(t *testing.T) {
	scheme := rawAutoscalerTestScheme(t)
	cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
	b := &BaseComponentFields{Client: cl, Scheme: scheme}
	ext := refExt(1)
	isvc := holdTestISVC(ext)
	ir := holdTestIR("llm-a-engine", storedIRBlock())

	require.NoError(t, dispatchIRAutoscaler(context.Background(), b, isvc, ir,
		&ext, ComponentAutoscaling{Hold: true}))

	so := &kedav1.ScaledObject{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
		Namespace: ir.Namespace,
		Name:      isvcutils.GetScaledObjectName(ir.Name),
	}, so), "the hold must dispatch the SO from the IR's stored block")
	assert.Equal(t, storedIRBlock().Keda.Triggers, so.Spec.Triggers,
		"trigger content comes from ir.Spec.Autoscaler, not the (nil) pass block")
	require.NotNil(t, so.Spec.ScaleTargetRef)
	assert.Equal(t, "InferenceReplica", so.Spec.ScaleTargetRef.Kind)
	assert.Equal(t, ir.Name, so.Spec.ScaleTargetRef.Name)
	require.NotNil(t, so.Spec.MinReplicaCount)
	assert.Equal(t, int32(1), *so.Spec.MinReplicaCount)
	require.NotNil(t, so.Spec.MaxReplicaCount)
	assert.Equal(t, int32(4), *so.Spec.MaxReplicaCount)
}

// TestDispatchIRAutoscaler_HoldWithoutStoredBlockDispatchesNothing pins the
// held-first-reconcile shape: with no stored block there is nothing to
// stand on, so the hold dispatches nothing — no ScaledObject, no default
// HPA.
func TestDispatchIRAutoscaler_HoldWithoutStoredBlockDispatchesNothing(t *testing.T) {
	scheme := rawAutoscalerTestScheme(t)
	cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
	b := &BaseComponentFields{Client: cl, Scheme: scheme}
	ext := refExt(1)
	isvc := holdTestISVC(ext)
	ir := holdTestIR("llm-a-engine", nil)

	require.NoError(t, dispatchIRAutoscaler(context.Background(), b, isvc, ir,
		&ext, ComponentAutoscaling{Hold: true}))

	so := &kedav1.ScaledObject{}
	err := cl.Get(context.Background(), types.NamespacedName{
		Namespace: ir.Namespace,
		Name:      isvcutils.GetScaledObjectName(ir.Name),
	}, so)
	assert.True(t, apierrors.IsNotFound(err), "no ScaledObject may appear without a stored block, got %v", err)
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	err = cl.Get(context.Background(), types.NamespacedName{Namespace: ir.Namespace, Name: ir.Name}, hpa)
	assert.True(t, apierrors.IsNotFound(err), "no default HPA may substitute a held policy, got %v", err)
}

// resolvedCondition extracts the AutoscalerResolved condition from the
// component's mirrored autoscaler status.
func resolvedCondition(t *testing.T, isvc *v1beta1.InferenceService, component v1beta1.ComponentType) *metav1.Condition {
	t.Helper()
	status := isvc.Status.Components[component]
	require.NotNil(t, status.Autoscaler)
	cond := apimeta.FindStatusCondition(status.Autoscaler.Conditions, v1beta1.AutoscalerResolvedCondition)
	require.NotNil(t, cond, "the AutoscalerResolved condition must be stamped for a ref-carrying component")
	return cond
}

// TestWriteComponentAutoscalerStatus_RenderedPolicyProvenance pins the
// rendered variant: specSource=policy, the provenance block carries the
// digests, the condition is True/RenderedFromPolicy, and a second
// identical pass keeps the condition's LastTransitionTime byte-stable.
func TestWriteComponentAutoscalerStatus_RenderedPolicyProvenance(t *testing.T) {
	cl := ctrlclientfake.NewClientBuilder().WithScheme(rawAutoscalerTestScheme(t)).Build()
	b := &BaseComponentFields{
		Piece:          render.Piece{DeploymentMode: constants.OMENative},
		Client:         cl,
		PolicyResolver: holdTestResolver(t, 0, holdTestPolicy("default")),
	}
	ext := refExt(1)
	isvc := holdTestISVC(ext)
	objectMeta := metav1.ObjectMeta{Name: "llm-a-engine", Namespace: "default"}

	require.NoError(t, writeComponentAutoscalerStatus(b, isvc, v1beta1.EngineComponent, objectMeta, &ext))

	status := isvc.Status.Components[v1beta1.EngineComponent]
	require.NotNil(t, status.Autoscaler)
	assert.Equal(t, string(autoscaler.SpecSourcePolicy), status.Autoscaler.SpecSource)
	assert.Equal(t, v1beta1.AutoscalerKEDA, status.Autoscaler.Class)
	require.NotNil(t, status.Autoscaler.Policy)
	assert.Equal(t, "request-activity-v1", status.Autoscaler.Policy.Name)
	assert.True(t, strings.HasPrefix(status.Autoscaler.Policy.PortableDigest, "pv1:"),
		"PortableDigest = %q", status.Autoscaler.Policy.PortableDigest)
	assert.NotEmpty(t, status.Autoscaler.Policy.ResolvedDigest)
	assert.Nil(t, status.Autoscaler.ShadowedPolicyRef,
		"a rendered ref has no inline block to shadow it")

	cond := resolvedCondition(t, isvc, v1beta1.EngineComponent)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, v1beta1.AutoscalerResolvedReasonRenderedFromPolicy, cond.Reason)
	firstTransition := cond.LastTransitionTime

	require.NotNil(t, status.ScaleTargetRef)
	assert.Equal(t, "InferenceReplica", status.ScaleTargetRef.Kind)
	assert.Equal(t, "llm-a-engine", status.ScaleTargetRef.Name)

	// Second identical pass: the seeded previous condition must keep its
	// LastTransitionTime, or the mirrored status would differ every
	// reconcile and storm status updates.
	require.NoError(t, writeComponentAutoscalerStatus(b, isvc, v1beta1.EngineComponent, objectMeta, &ext))
	again := resolvedCondition(t, isvc, v1beta1.EngineComponent)
	assert.Equal(t, metav1.ConditionTrue, again.Status)
	assert.True(t, again.LastTransitionTime.Equal(&firstTransition),
		"an unchanged condition must keep its LastTransitionTime across passes")
}

// TestWriteComponentAutoscalerStatus_HoldKeepsLastClassAndProvenance pins
// the hold variant: the live scaler (last-known-good) keeps reporting
// through the previously observed class, the last successful provenance
// stays visible, and the condition flips False with the hold reason.
func TestWriteComponentAutoscalerStatus_HoldKeepsLastClassAndProvenance(t *testing.T) {
	cl := ctrlclientfake.NewClientBuilder().WithScheme(rawAutoscalerTestScheme(t)).Build()
	b := &BaseComponentFields{
		Piece:          render.Piece{DeploymentMode: constants.OMENative},
		Client:         cl,
		PolicyResolver: holdTestResolver(t, 0, holdTestPolicy("default")),
	}
	ext := refExt(1)
	isvc := holdTestISVC(ext)
	objectMeta := metav1.ObjectMeta{Name: "llm-a-engine", Namespace: "default"}

	// Pass 1: the policy renders and seeds the provenance.
	require.NoError(t, writeComponentAutoscalerStatus(b, isvc, v1beta1.EngineComponent, objectMeta, &ext))
	rendered := isvc.Status.Components[v1beta1.EngineComponent].Autoscaler
	require.NotNil(t, rendered.Policy)

	// Pass 2: the policy object is gone — the resolver holds.
	b.PolicyResolver = holdTestResolver(t, 0)
	require.NoError(t, writeComponentAutoscalerStatus(b, isvc, v1beta1.EngineComponent, objectMeta, &ext))

	status := isvc.Status.Components[v1beta1.EngineComponent]
	require.NotNil(t, status.Autoscaler)
	assert.Equal(t, v1beta1.AutoscalerKEDA, status.Autoscaler.Class,
		"the hold mirrors through the previously reported class, not ManagedBy=none")
	assert.Equal(t, string(autoscaler.SpecSourcePolicy), status.Autoscaler.SpecSource)
	require.NotNil(t, status.Autoscaler.Policy,
		"the last successful provenance stays visible so operators see WHICH render is standing")
	assert.Equal(t, rendered.Policy.PortableDigest, status.Autoscaler.Policy.PortableDigest)
	assert.Equal(t, rendered.Policy.ResolvedDigest, status.Autoscaler.Policy.ResolvedDigest)

	cond := resolvedCondition(t, isvc, v1beta1.EngineComponent)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1beta1.AutoscalerResolvedReasonPolicyNotFound, cond.Reason)
	assert.Contains(t, cond.Message, "holding last-known-good")
}

// TestWriteComponentAutoscalerStatus_InlineShadowsPolicy pins the
// inline-precedence variant: the inline block wins (specSource=isvc,
// condition True/InlinePrecedence) and the shadow fields preview what the
// ref WOULD render.
func TestWriteComponentAutoscalerStatus_InlineShadowsPolicy(t *testing.T) {
	cl := ctrlclientfake.NewClientBuilder().WithScheme(rawAutoscalerTestScheme(t)).Build()
	b := &BaseComponentFields{
		Piece:          render.Piece{DeploymentMode: constants.OMENative},
		Client:         cl,
		PolicyResolver: holdTestResolver(t, 0, holdTestPolicy("default")),
	}
	ext := refExt(1)
	ext.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}
	isvc := holdTestISVC(ext)
	objectMeta := metav1.ObjectMeta{Name: "llm-a-engine", Namespace: "default"}

	require.NoError(t, writeComponentAutoscalerStatus(b, isvc, v1beta1.EngineComponent, objectMeta, &ext))

	status := isvc.Status.Components[v1beta1.EngineComponent]
	require.NotNil(t, status.Autoscaler)
	assert.Equal(t, string(autoscaler.SpecSourceISVC), status.Autoscaler.SpecSource)
	assert.Equal(t, v1beta1.AutoscalerHPA, status.Autoscaler.Class,
		"the inline block outranks the rendered policy")
	require.NotNil(t, status.Autoscaler.ShadowedPolicyRef)
	assert.Equal(t, "request-activity-v1", status.Autoscaler.ShadowedPolicyRef.Name)
	assert.True(t, strings.HasPrefix(status.Autoscaler.ShadowedPolicyRef.PortableDigest, "pv1:"),
		"PortableDigest = %q", status.Autoscaler.ShadowedPolicyRef.PortableDigest)
	assert.NotEmpty(t, status.Autoscaler.ShadowedPolicyRef.WouldRenderDigest,
		"the shadow previews what removing the inline block would dispatch")

	cond := resolvedCondition(t, isvc, v1beta1.EngineComponent)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, v1beta1.AutoscalerResolvedReasonInlinePrecedence, cond.Reason)
}

// TestWriteComponentAutoscalerStatus_MultiNodeRefIsInert pins the
// unsupported-mode variant: MultiNode has no autoscaler dispatch, so a ref
// never reaches the resolver (nil PolicyResolver proves it) and surfaces
// only as False/UnsupportedDeploymentMode with no published scale target.
func TestWriteComponentAutoscalerStatus_MultiNodeRefIsInert(t *testing.T) {
	cl := ctrlclientfake.NewClientBuilder().WithScheme(rawAutoscalerTestScheme(t)).Build()
	b := &BaseComponentFields{Piece: render.Piece{DeploymentMode: constants.MultiNode}, Client: cl}
	ext := refExt(1)
	isvc := holdTestISVC(ext)
	objectMeta := metav1.ObjectMeta{Name: "llm-a-engine", Namespace: "default"}

	require.NoError(t, writeComponentAutoscalerStatus(b, isvc, v1beta1.EngineComponent, objectMeta, &ext))

	status := isvc.Status.Components[v1beta1.EngineComponent]
	require.NotNil(t, status.Autoscaler)
	assert.Equal(t, string(autoscaler.SpecSourceDefault), status.Autoscaler.SpecSource,
		"with the ref inert the ordinary chain resolves below the policy layer")
	assert.Nil(t, status.Autoscaler.Policy)
	assert.Nil(t, status.ScaleTargetRef,
		"MultiNode publishes no scale target")

	cond := resolvedCondition(t, isvc, v1beta1.EngineComponent)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1beta1.AutoscalerResolvedReasonUnsupportedMode, cond.Reason)
	assert.Contains(t, cond.Message, "inert")
}
