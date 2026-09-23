package irprojector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	knapis "knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	isvcstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/status"
)

func TestAggregateIRStatusLeavesRehydrationRetirementToProvisioningClient(t *testing.T) {
	for _, marker := range []string{"1", "", "completed:1", "2"} {
		t.Run("marker="+marker, func(t *testing.T) {
			ctx := context.Background()
			isvc := baselineISVC("endpoint", "customer")
			isvc.Annotations = map[string]string{constants.ArtifactRehydrationGenerationAnnotation: marker, "keep": "value"}
			isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{Type: v1beta1.IngressReady, Status: corev1.ConditionTrue})
			isvc.Status.SetCondition(v1beta1.EngineReady, &knapis.Condition{Type: v1beta1.EngineReady, Status: corev1.ConditionFalse})
			ir := liveIR(isvc.Name, isvc.Namespace, 1)
			base := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(isvc, ir).WithStatusSubresource(isvc, ir).Build()
			patches, readyWrites := 0, 0
			c := interceptor.NewClient(base, interceptor.Funcs{
				Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					patches++
					return cl.Patch(ctx, obj, patch, opts...)
				},
				SubResourceUpdate: func(ctx context.Context, cl client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if service, ok := obj.(*v1beta1.InferenceService); ok && isvcstatus.IsReadyTrue(service.Status) {
						readyWrites++
						latest := &v1beta1.InferenceService{}
						require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(service), latest))
						require.Equal(t, marker, latest.Annotations[constants.ArtifactRehydrationGenerationAnnotation])
						require.Empty(t, latest.Annotations[constants.ArtifactRehydrationCompletedAtAnnotation])
					}
					return cl.SubResource(subresource).Update(ctx, obj, opts...)
				},
			})
			require.NoError(t, AggregateIRStatus(ctx, c, base, isvc, engineOMENativeModes))
			require.Positive(t, readyWrites)
			latest := &v1beta1.InferenceService{}
			require.NoError(t, base.Get(ctx, client.ObjectKeyFromObject(isvc), latest))
			require.True(t, isvcstatus.IsReadyTrue(latest.Status))
			require.Equal(t, "value", latest.Annotations["keep"])
			require.Zero(t, patches, "readiness must not mutate MP-owned annotations")
			require.Equal(t, marker, latest.Annotations[constants.ArtifactRehydrationGenerationAnnotation])
		})
	}
}

func TestAggregateIRStatusDoesNotPatchCompletionMetadata(t *testing.T) {
	ctx := context.Background()
	isvc := baselineISVC("endpoint", "customer")
	isvc.Annotations = map[string]string{constants.ArtifactRehydrationGenerationAnnotation: "1"}
	isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{Type: v1beta1.IngressReady, Status: corev1.ConditionTrue})
	isvc.Status.SetCondition(v1beta1.EngineReady, &knapis.Condition{Type: v1beta1.EngineReady, Status: corev1.ConditionFalse})
	ir := liveIR(isvc.Name, isvc.Namespace, 1)
	base := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(isvc, ir).WithStatusSubresource(isvc, ir).Build()
	c := interceptor.NewClient(base, interceptor.Funcs{Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
		t.Fatal("readiness must not patch MP-owned completion metadata")
		return nil
	}})
	require.NoError(t, AggregateIRStatus(ctx, c, base, isvc, engineOMENativeModes))
	latest := &v1beta1.InferenceService{}
	require.NoError(t, base.Get(ctx, client.ObjectKeyFromObject(isvc), latest))
	require.True(t, isvcstatus.IsReadyTrue(latest.Status))
	require.Equal(t, "1", latest.Annotations[constants.ArtifactRehydrationGenerationAnnotation])
	require.Empty(t, latest.Annotations[constants.ArtifactRehydrationCompletedAtAnnotation])
}

func TestAggregateIRStatusUsesCurrentPassReadiness(t *testing.T) {
	for _, conditionType := range []knapis.ConditionType{v1beta1.IngressReady, v1beta1.EngineReady} {
		for _, conditionStatus := range []corev1.ConditionStatus{corev1.ConditionFalse, corev1.ConditionUnknown} {
			t.Run(string(conditionType)+"/"+string(conditionStatus), func(t *testing.T) {
				ctx := context.Background()
				isvc := baselineISVC("endpoint", "customer")
				isvc.Annotations = map[string]string{constants.ArtifactRehydrationGenerationAnnotation: "1"}
				isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{Status: corev1.ConditionTrue})
				isvc.Status.SetCondition(v1beta1.EngineReady, &knapis.Condition{Status: corev1.ConditionTrue})
				isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
					v1beta1.DecoderComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 7}},
				}
				ir := liveIR(isvc.Name, isvc.Namespace, 1)
				modes := engineOMENativeModes
				if conditionType == v1beta1.EngineReady {
					isvc.Spec.Router = &v1beta1.RouterSpec{}
					ir.Name = InferenceReplicaName(isvc.Name, v1beta1.RouterComponent)
					ir.Spec.Component = v1beta1.RouterComponent
					modes = map[v1beta1.ComponentType]constants.DeploymentModeType{
						v1beta1.EngineComponent: constants.RawDeployment,
						v1beta1.RouterComponent: constants.OMENative,
					}
				} else {
					isvc.Status.SetCondition(v1beta1.EngineReady, &knapis.Condition{Status: corev1.ConditionFalse})
				}
				base := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(isvc, ir).WithStatusSubresource(isvc, ir).Build()
				// Ingress/direct-component reconciliation updates only the caller;
				// the live peer lifecycle is newer than this reconcile's snapshot.
				isvc.Status.Components[v1beta1.DecoderComponent].Lifecycle.Replicas = 1
				isvc.Status.SetCondition(conditionType, &knapis.Condition{Status: conditionStatus, Reason: "CurrentPassPending", Message: "not ready this pass"})
				patches, readyWrites, statusWrites := 0, 0, 0
				c := interceptor.NewClient(base, interceptor.Funcs{
					Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
						patches++
						return cl.Patch(ctx, obj, patch, opts...)
					},
					SubResourceUpdate: func(ctx context.Context, cl client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
						statusWrites++
						if isvcstatus.IsReadyTrue(obj.(*v1beta1.InferenceService).Status) {
							readyWrites++
						}
						if statusWrites == 1 {
							peer := &v1beta1.InferenceService{}
							require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(isvc), peer))
							peer.Status.Components[v1beta1.DecoderComponent].Lifecycle.Replicas = 9
							require.NoError(t, cl.Status().Update(ctx, peer))
						}
						return cl.SubResource(subresource).Update(ctx, obj, opts...)
					},
				})
				require.NoError(t, AggregateIRStatus(ctx, c, base, isvc, modes))
				latest := &v1beta1.InferenceService{}
				require.NoError(t, base.Get(ctx, client.ObjectKeyFromObject(isvc), latest))
				require.Equal(t, "1", latest.Annotations[constants.ArtifactRehydrationGenerationAnnotation])
				require.Empty(t, latest.Annotations[constants.ArtifactRehydrationCompletedAtAnnotation])
				require.Zero(t, patches)
				require.Zero(t, readyWrites, "must not publish even an intermediate Ready status")
				require.GreaterOrEqual(t, statusWrites, 2, "must retry the peer status conflict")
				require.False(t, isvcstatus.IsReadyTrue(latest.Status))
				condition := latest.Status.GetCondition(conditionType)
				require.Equal(t, conditionStatus, condition.Status)
				require.Equal(t, "CurrentPassPending", condition.Reason)
				require.Equal(t, "not ready this pass", condition.Message)
				require.EqualValues(t, 9, latest.Status.Components[v1beta1.DecoderComponent].Lifecycle.Replicas)

				isvc.Status.SetCondition(conditionType, &knapis.Condition{Status: corev1.ConditionTrue})
				require.NoError(t, AggregateIRStatus(ctx, c, base, isvc, modes))
				require.NoError(t, base.Get(ctx, client.ObjectKeyFromObject(isvc), latest))
				require.True(t, isvcstatus.IsReadyTrue(latest.Status))
				require.Equal(t, "1", latest.Annotations[constants.ArtifactRehydrationGenerationAnnotation])
				require.Empty(t, latest.Annotations[constants.ArtifactRehydrationCompletedAtAnnotation])
				require.Zero(t, patches)
				require.EqualValues(t, 9, latest.Status.Components[v1beta1.DecoderComponent].Lifecycle.Replicas)
			})
		}
	}
}

func TestAggregateIRStatusRouterAndDecoderDoNotGateReady(t *testing.T) {
	ctx := context.Background()
	isvc := baselineISVC("endpoint", "customer")
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	isvc.Annotations = map[string]string{constants.ArtifactRehydrationGenerationAnnotation: "1"}
	isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{Status: corev1.ConditionTrue})
	isvc.Status.SetCondition(v1beta1.EngineReady, &knapis.Condition{Status: corev1.ConditionFalse})
	isvc.Status.SetCondition(v1beta1.DecoderReady, &knapis.Condition{Status: corev1.ConditionTrue})
	isvc.Status.SetCondition(v1beta1.RouterReady, &knapis.Condition{Status: corev1.ConditionTrue})
	engine := liveIR(isvc.Name, isvc.Namespace, 1)
	objects := []client.Object{isvc, engine}
	modes := map[v1beta1.ComponentType]constants.DeploymentModeType{v1beta1.EngineComponent: constants.OMENative}
	for _, component := range []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.RouterComponent} {
		ir := liveIR(isvc.Name, isvc.Namespace, 1)
		ir.Name = InferenceReplicaName(isvc.Name, component)
		ir.Spec.Component = component
		ir.Status.ReadyReplicas = 0
		ir.Status.ServingReplicas = 0
		objects = append(objects, ir)
		modes[component] = constants.OMENative
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objects...).WithStatusSubresource(isvc, engine).Build()
	require.NoError(t, AggregateIRStatus(ctx, c, c, isvc, modes))
	latest := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(isvc), latest))
	require.Equal(t, corev1.ConditionFalse, latest.Status.GetCondition(v1beta1.DecoderReady).Status)
	require.Equal(t, corev1.ConditionFalse, latest.Status.GetCondition(v1beta1.RouterReady).Status)
	require.True(t, isvcstatus.IsReadyTrue(latest.Status), "the existing Ready contract depends only on ingress and engine")
	require.Equal(t, "1", latest.Annotations[constants.ArtifactRehydrationGenerationAnnotation])
}
