package components

import (
	"context"
	"testing"
	"time"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/autoscalerpolicy/render"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func holdTestResolver(t *testing.T, holdRequeue time.Duration, objs ...client.Object) *autoscaler.PolicyResolver {
	t.Helper()
	return &autoscaler.PolicyResolver{
		Client:           componentClient(t, fakeclient.WithObjects(objs...)),
		Providers:        render.Providers{"cluster-prometheus": {ServerAddress: "http://prometheus.example.com:9090"}},
		Enabled:          true,
		KedaAvailable:    true,
		HoldRequeueAfter: holdRequeue,
	}
}

// holdTestPolicy renders a KEDA trigger whose query embeds {{ .MinReplicas }}
// so the bounds fed to the render are observable in the output.
func holdTestPolicy(namespace string) *v1beta1.AutoscalerPolicy {
	return &v1beta1.AutoscalerPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "request-activity-v1", Namespace: namespace, Generation: 1},
		Spec: v1beta1.AutoscalerPolicySpec{
			Class: v1beta1.AutoscalerKEDA,
			Keda: &v1beta1.KedaPolicyTemplate{
				Triggers: []v1beta1.KedaTriggerTemplate{{
					Type:        "prometheus",
					ProviderRef: &v1beta1.MetricProviderRef{Name: "cluster-prometheus"},
					Metadata: map[string]string{
						"threshold":        "1",
						"ignoreNullValues": "false",
						"query":            `((sum({inferenceservice="{{ .ISVCName }}"}) > bool 0) * {{ .MinReplicas }})`,
					},
				}},
			},
		},
	}
}

func holdTestISVC(ext v1beta1.ComponentExtensionSpec) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "llm-a", Namespace: "default", UID: types.UID("llm-a-uid")},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{ComponentExtensionSpec: ext},
		},
	}
}

func refExt(minReplicas int) v1beta1.ComponentExtensionSpec {
	return v1beta1.ComponentExtensionSpec{
		MinReplicas:         ptr.To(minReplicas),
		MaxReplicas:         4,
		AutoscalerPolicyRef: &v1beta1.AutoscalerPolicyRef{Name: "request-activity-v1"},
	}
}

func TestResolveComponentAutoscaling_HoldRequeueAfter(t *testing.T) {
	cases := []struct {
		name string
		ttl  time.Duration
		want time.Duration
	}{
		{"config TTL drives the hold requeue", 42 * time.Second, 42 * time.Second},
		{"disabled TTL carries no periodic requeue", 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// No stored policy: the ref holds with PolicyNotFound.
			b := &BaseComponentFields{PolicyResolver: holdTestResolver(t, tc.ttl)}
			ext := refExt(1)
			isvc := holdTestISVC(ext)

			res, err := resolveComponentAutoscaling(context.Background(), b, isvc, v1beta1.EngineComponent, &ext)
			require.NoError(t, err)
			assert.True(t, res.Hold)
			assert.Equal(t, tc.want, res.RequeueAfter)

			raw, err := resolveRawComponentAutoscaling(context.Background(), b, isvc, v1beta1.EngineComponent, &ext, nil)
			require.NoError(t, err)
			assert.True(t, raw.Hold)
			assert.Equal(t, tc.want, raw.RequeueAfter)
		})
	}

	// A successful render never asks for the periodic requeue.
	t.Run("rendered outcome carries no requeue", func(t *testing.T) {
		b := &BaseComponentFields{PolicyResolver: holdTestResolver(t, 42*time.Second, holdTestPolicy("default"))}
		ext := refExt(1)
		isvc := holdTestISVC(ext)

		res, err := resolveComponentAutoscaling(context.Background(), b, isvc, v1beta1.EngineComponent, &ext)
		require.NoError(t, err)
		assert.False(t, res.Hold)
		assert.Zero(t, res.RequeueAfter)
	})
}

func TestMergeRequeueAfter(t *testing.T) {
	cases := []struct {
		name   string
		result ctrl.Result
		after  time.Duration
		want   time.Duration
	}{
		{"zero hold requeue keeps the result", ctrl.Result{RequeueAfter: 10 * time.Second}, 0, 10 * time.Second},
		{"hold requeue fills an empty result", ctrl.Result{}, 30 * time.Second, 30 * time.Second},
		{"sooner dispatcher poll wins", ctrl.Result{RequeueAfter: 10 * time.Second}, 30 * time.Second, 10 * time.Second},
		{"sooner hold requeue wins", ctrl.Result{RequeueAfter: 30 * time.Second}, 10 * time.Second, 10 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, mergeRequeueAfter(tc.result, tc.after).RequeueAfter)
		})
	}
}

// A raw hold's requeue must survive the full component dispatch: the
// reconcile result carries it, and no scaler object appears (fail closed).
func TestEngineRawDeployment_HoldRequeueSurfaces(t *testing.T) {
	cl := componentClient(t)
	resolver := &autoscaler.PolicyResolver{
		Client:           cl,
		Enabled:          true,
		KedaAvailable:    true,
		HoldRequeueAfter: 30 * time.Second,
	}
	isvc := holdTestISVC(refExt(1))
	componentMeta := metav1.ObjectMeta{
		Name:        isvc.Name + "-engine",
		Namespace:   isvc.Namespace,
		Annotations: map[string]string{},
	}
	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: constants.MainContainerName, Image: "example.com/runtime:test"}},
	}
	deps := &ComponentDeps{
		Client:    cl,
		Clientset: rawAutoscalerTestClientset(),
		Scheme:    cl.Scheme(),
		Config:    &controllerconfig.InferenceServicesConfig{},
	}
	engine := NewEngine(deps, ComponentInputs{DeploymentMode: constants.RawDeployment, PolicyResolver: resolver}, isvc.Spec.Engine).(*Engine)
	request := mustComponentPDBRequest(t, &engine.BaseComponentFields, isvc, constants.RawDeployment, v1beta1.EngineComponent, componentMeta, &isvc.Spec.Engine.ComponentExtensionSpec)

	result, err := engine.reconcileDeployment(context.Background(), isvc, componentMeta, podSpec, 0, nil, request)
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, result.RequeueAfter)

	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	err = cl.Get(context.Background(), types.NamespacedName{Namespace: componentMeta.Namespace, Name: componentMeta.Name}, hpa)
	assert.True(t, apierrors.IsNotFound(err), "a hold without a record must not create an HPA")
	so := &kedav1.ScaledObject{}
	err = cl.Get(context.Background(), types.NamespacedName{Namespace: componentMeta.Namespace, Name: isvcutils.GetScaledObjectName(componentMeta.Name)}, so)
	assert.True(t, apierrors.IsNotFound(err), "a hold without a record must not create a ScaledObject")
}

// A raw component declaring min 0 with a typed-KEDA policy must render its
// {{ .MinReplicas }} literal as 0 — the same value the dispatch stamps.
func TestResolveRawComponentAutoscaling_MinZeroRendersZero(t *testing.T) {
	b := &BaseComponentFields{PolicyResolver: holdTestResolver(t, 0, holdTestPolicy("default"))}
	ext := refExt(0)
	isvc := holdTestISVC(ext)

	raw, err := resolveRawComponentAutoscaling(context.Background(), b, isvc, v1beta1.EngineComponent, &ext, nil)
	require.NoError(t, err)
	assert.False(t, raw.Hold)
	assert.True(t, raw.FromPolicy)
	require.NotNil(t, raw.Autoscaler)
	require.NotNil(t, raw.Autoscaler.Keda)
	require.NotEmpty(t, raw.Autoscaler.Keda.Triggers)
	assert.Contains(t, raw.Autoscaler.Keda.Triggers[0].Metadata["query"], "* 0)")
}

// The AutoscalerPolicyHold warning fires on transition only: once the
// component status reports the same hold reason, later passes stay silent.
func TestHoldEventDedupedByStatusCondition(t *testing.T) {
	recorder := record.NewFakeRecorder(8)
	b := &BaseComponentFields{
		Recorder:       recorder,
		PolicyResolver: holdTestResolver(t, 0),
	}
	ext := refExt(1)
	isvc := holdTestISVC(ext)

	drain := func() []string {
		var events []string
		for {
			select {
			case e := <-recorder.Events:
				events = append(events, e)
			default:
				return events
			}
		}
	}

	_, err := resolveComponentAutoscaling(context.Background(), b, isvc, v1beta1.EngineComponent, &ext)
	require.NoError(t, err)
	first := drain()
	require.Len(t, first, 1)
	assert.Contains(t, first[0], "AutoscalerPolicyHold")

	// The status writer stamps AutoscalerResolved=False with the hold reason;
	// with that recorded, a second pass over the same hold is not a transition.
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Autoscaler: &v1beta1.ComponentAutoscalerStatus{
				Conditions: []metav1.Condition{{
					Type:   v1beta1.AutoscalerResolvedCondition,
					Status: metav1.ConditionFalse,
					Reason: v1beta1.AutoscalerResolvedReasonPolicyNotFound,
				}},
			},
		},
	}
	_, err = resolveComponentAutoscaling(context.Background(), b, isvc, v1beta1.EngineComponent, &ext)
	require.NoError(t, err)
	assert.Empty(t, drain(), "same hold reason must not re-fire the event")

	raw, err := resolveRawComponentAutoscaling(context.Background(), b, isvc, v1beta1.EngineComponent, &ext, nil)
	require.NoError(t, err)
	assert.True(t, raw.Hold)
	assert.Empty(t, drain(), "raw path shares the dedupe")

	// A different hold reason is a fresh transition and must be reported.
	status := isvc.Status.Components[v1beta1.EngineComponent]
	status.Autoscaler.Conditions[0].Reason = v1beta1.AutoscalerResolvedReasonClassUnavailable
	isvc.Status.Components[v1beta1.EngineComponent] = status
	_, err = resolveComponentAutoscaling(context.Background(), b, isvc, v1beta1.EngineComponent, &ext)
	require.NoError(t, err)
	assert.Len(t, drain(), 1)
}
