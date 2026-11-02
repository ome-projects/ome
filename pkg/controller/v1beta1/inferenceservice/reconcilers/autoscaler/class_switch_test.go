package autoscaler

import (
	"context"
	"errors"
	"slices"
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/keda"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

const (
	switchNS   = "test-ns"
	switchName = "demo-engine"
)

var (
	switchHPAKey = types.NamespacedName{Namespace: switchNS, Name: switchName}
	switchSOKey  = types.NamespacedName{Namespace: switchNS, Name: utils.GetScaledObjectName(switchName)}
)

// scalerWrites records every create, update and delete of an HPA or a
// ScaledObject, in order, as "<verb> <kind> <name>"; dry-run creates are
// recorded as "validate HorizontalPodAutoscaler <generateName>".
type scalerWrites struct{ log []string }

func (w *scalerWrites) funcs() interceptor.Funcs {
	return interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			createOpts := &client.CreateOptions{}
			createOpts.ApplyOptions(opts)
			if kind := scalerKind(obj); kind != "" {
				if slices.Contains(createOpts.DryRun, metav1.DryRunAll) {
					w.log = append(w.log, "validate "+kind+" "+obj.GetGenerateName())
				} else {
					w.log = append(w.log, "create "+kind+" "+obj.GetName())
				}
			}
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if kind := scalerKind(obj); kind != "" {
				w.log = append(w.log, "update "+kind+" "+obj.GetName())
			}
			return c.Update(ctx, obj, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if kind := scalerKind(obj); kind != "" {
				w.log = append(w.log, "delete "+kind+" "+obj.GetName())
			}
			return c.Delete(ctx, obj, opts...)
		},
	}
}

func scalerKind(obj client.Object) string {
	switch obj.(type) {
	case *autoscalingv2.HorizontalPodAutoscaler:
		return "HorizontalPodAutoscaler"
	case *kedav1.ScaledObject:
		return "ScaledObject"
	default:
		return ""
	}
}

func switchClient(t *testing.T, writes *scalerWrites, objs ...client.Object) client.Client {
	t.Helper()
	builder := fake.NewClientBuilder().
		WithScheme(dispatchScheme(t)).
		WithStatusSubresource(&kedav1.ScaledObject{}).
		WithObjects(objs...)
	if writes != nil {
		builder = builder.WithInterceptorFuncs(writes.funcs())
	}
	return builder.Build()
}

func switchParams(c client.Client, autoscaler *v1beta1.ComponentAutoscaler) DispatchParams {
	return DispatchParams{
		Client:         c,
		Scheme:         c.Scheme(),
		Owner:          dispatchOwner(switchName),
		Namespace:      switchNS,
		Name:           switchName,
		ScaleTargetRef: dispatchScaleTargetRef(switchName),
		Autoscaler:     autoscaler,
		MinReplicas:    1,
		MaxReplicas:    5,
	}
}

func kedaClass() *v1beta1.ComponentAutoscaler {
	return &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()}
}

func kedaClassWithHPAName(name string) *v1beta1.ComponentAutoscaler {
	autoscaler := kedaClass()
	autoscaler.Keda.Advanced = &kedav1.AdvancedConfig{
		HorizontalPodAutoscalerConfig: &kedav1.HorizontalPodAutoscalerConfig{Name: name},
	}
	return autoscaler
}

func hpaClass() *v1beta1.ComponentAutoscaler {
	return &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}
}

// workingHPA is the HPA OME runs for class hpa.
func workingHPA() *autoscalingv2.HorizontalPodAutoscaler {
	hpa := existingHPA(switchNS, switchName, dispatchOwner(switchName))
	hpa.UID = types.UID("working-hpa-uid")
	return hpa
}

func requirePending(t *testing.T, err error, reason string) {
	t.Helper()
	var pending *TransitionPendingError
	require.True(t, errors.As(err, &pending), "expected a pending switch, got %v", err)
	assert.Equal(t, reason, pending.Reason)
}

func getSwitchHPA(t *testing.T, c client.Client) *autoscalingv2.HorizontalPodAutoscaler {
	t.Helper()
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	require.NoError(t, c.Get(context.Background(), switchHPAKey, hpa))
	return hpa
}

func getSwitchSO(t *testing.T, c client.Client) *kedav1.ScaledObject {
	t.Helper()
	so := &kedav1.ScaledObject{}
	require.NoError(t, c.Get(context.Background(), switchSOKey, so))
	return so
}

// scaledObjectControllerRef is the controller reference KEDA stamps on the
// HPAs it manages.
func scaledObjectControllerRef(so *kedav1.ScaledObject) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion:         kedav1.GroupVersion.String(),
		Kind:               "ScaledObject",
		Name:               so.Name,
		UID:                so.UID,
		Controller:         ptr.To(true),
		BlockOwnerDeletion: ptr.To(true),
	}
}

// simulateKEDATakeover does what KEDA's ScaledObject controller does to the
// HPA a ScaledObject names: it updates that HPA in place, so the UID stays,
// replacing the controller with the ScaledObject and the metrics with the
// ScaledObject's triggers.
func simulateKEDATakeover(t *testing.T, c client.Client, so *kedav1.ScaledObject, hpa *autoscalingv2.HorizontalPodAutoscaler) {
	t.Helper()
	live := &autoscalingv2.HorizontalPodAutoscaler{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(hpa), live))
	live.OwnerReferences = []metav1.OwnerReference{scaledObjectControllerRef(so)}
	live.Labels = map[string]string{"app.kubernetes.io/managed-by": "keda-operator"}
	live.Annotations = so.Annotations
	live.Spec.ScaleTargetRef = autoscalingv2.CrossVersionObjectReference{
		APIVersion: so.Spec.ScaleTargetRef.APIVersion,
		Kind:       so.Spec.ScaleTargetRef.Kind,
		Name:       so.Spec.ScaleTargetRef.Name,
	}
	live.Spec.Metrics = []autoscalingv2.MetricSpec{{
		Type: autoscalingv2.ExternalMetricSourceType,
		External: &autoscalingv2.ExternalMetricSource{
			Metric: autoscalingv2.MetricIdentifier{Name: "s0-cron"},
			Target: autoscalingv2.MetricTarget{Type: autoscalingv2.AverageValueMetricType},
		},
	}}
	require.NoError(t, c.Update(context.Background(), live))
}

// kedaCreatedHPA is an HPA KEDA created for the ScaledObject.
func kedaCreatedHPA(name string, so *kedav1.ScaledObject) *autoscalingv2.HorizontalPodAutoscaler {
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       switchNS,
			Name:            name,
			UID:             types.UID(name + "-uid"),
			OwnerReferences: []metav1.OwnerReference{scaledObjectControllerRef(so)},
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: dispatchScaleTargetRef(switchName),
			MaxReplicas:    5,
		},
	}
}

func markScaledObjectReady(t *testing.T, c client.Client, key types.NamespacedName) {
	t.Helper()
	so := &kedav1.ScaledObject{}
	require.NoError(t, c.Get(context.Background(), key, so))
	so.Status.Conditions = kedav1.Conditions{
		{Type: kedav1.ConditionReady, Status: metav1.ConditionTrue, Reason: kedav1.ScaledObjectConditionReadySuccessReason},
	}
	so.Status.HpaName = keda.ConfiguredHPAName(so.Spec.Advanced)
	require.NoError(t, c.Status().Update(context.Background(), so))
}

// takenOverState returns a client in which the ScaledObject took over the
// HPA named after the Component and KEDA reports it Ready.
func takenOverState(t *testing.T, writes *scalerWrites) client.Client {
	t.Helper()
	c := switchClient(t, nil, workingHPA())
	err := DispatchAutoscaler(context.Background(), switchParams(c, kedaClass()))
	requirePending(t, err, v1beta1.AutoscalerSwitchedReasonWaitingForKEDA)
	simulateKEDATakeover(t, c, getSwitchSO(t, c), workingHPA())
	markScaledObjectReady(t, c, switchSOKey)
	if writes == nil {
		return c
	}
	objs := []client.Object{getSwitchHPA(t, c), getSwitchSO(t, c)}
	for _, obj := range objs {
		obj.SetResourceVersion("")
	}
	return switchClient(t, writes, objs...)
}

func TestKEDATakeover_NewScaledObjectNamesTheWorkingHPA(t *testing.T) {
	writes := &scalerWrites{}
	c := switchClient(t, writes, workingHPA())
	before := getSwitchHPA(t, c)

	err := DispatchAutoscaler(context.Background(), switchParams(c, kedaClass()))
	requirePending(t, err, v1beta1.AutoscalerSwitchedReasonWaitingForKEDA)

	assert.Equal(t, []string{"create ScaledObject " + switchSOKey.Name}, writes.log, "OME writes only the ScaledObject")
	so := getSwitchSO(t, c)
	assert.Equal(t, "true", so.Annotations[kedav1.ScaledObjectTransferHpaOwnershipAnnotation])
	require.NotNil(t, so.Spec.Advanced)
	require.NotNil(t, so.Spec.Advanced.HorizontalPodAutoscalerConfig)
	assert.Equal(t, switchName, so.Spec.Advanced.HorizontalPodAutoscalerConfig.Name)
	assert.Equal(t, dispatchOwner(switchName), *metav1.GetControllerOf(so))

	after := getSwitchHPA(t, c)
	assert.Equal(t, before.UID, after.UID)
	assert.Equal(t, before.ResourceVersion, after.ResourceVersion, "the HPA keeps scaling untouched until KEDA takes it over")
}

func TestKEDATakeover_RequestedKEDABlockStaysUnchanged(t *testing.T) {
	c := switchClient(t, nil, workingHPA())
	autoscaler := kedaClass()
	requested := autoscaler.DeepCopy()

	err := DispatchAutoscaler(context.Background(), switchParams(c, autoscaler))
	requirePending(t, err, v1beta1.AutoscalerSwitchedReasonWaitingForKEDA)
	assert.Equal(t, requested, autoscaler, "the takeover name is set on a copy, never on the resolved block")
}

func TestKEDATakeover_CompletesWhenKEDAControlsTheHPAAndIsReady(t *testing.T) {
	c := switchClient(t, nil, workingHPA())
	params := switchParams(c, kedaClass())
	requirePending(t, DispatchAutoscaler(context.Background(), params), v1beta1.AutoscalerSwitchedReasonWaitingForKEDA)

	simulateKEDATakeover(t, c, getSwitchSO(t, c), workingHPA())
	requirePending(t, DispatchAutoscaler(context.Background(), params), v1beta1.AutoscalerSwitchedReasonScaledObjectNotReady)

	markScaledObjectReady(t, c, switchSOKey)
	require.NoError(t, DispatchAutoscaler(context.Background(), params))
	assert.Equal(t, types.UID("working-hpa-uid"), getSwitchHPA(t, c).UID)
}

func TestKEDATakeover_WaitsForKEDAToRetargetTheHPA(t *testing.T) {
	c := takenOverState(t, nil)
	hpa := getSwitchHPA(t, c)
	hpa.Spec.ScaleTargetRef = dispatchDeploymentTargetRef(switchName)
	require.NoError(t, c.Update(context.Background(), hpa))

	requirePending(t, DispatchAutoscaler(context.Background(), switchParams(c, kedaClass())), v1beta1.AutoscalerSwitchedReasonWaitingForKEDA)
}

// TestKEDATakeover_RepeatedReconcilesAreNoOps covers the steady state after a
// takeover and a controller restart: dispatch reads everything from live
// objects, keeps naming the HPA, and writes nothing.
func TestKEDATakeover_RepeatedReconcilesAreNoOps(t *testing.T) {
	writes := &scalerWrites{}
	c := takenOverState(t, writes)
	before := getSwitchSO(t, c)

	for i := 0; i < 3; i++ {
		require.NoError(t, DispatchAutoscaler(context.Background(), switchParams(c, kedaClass())))
	}
	assert.Empty(t, writes.log)
	after := getSwitchSO(t, c)
	assert.Equal(t, before.ResourceVersion, after.ResourceVersion)
	assert.Equal(t, switchName, after.Spec.Advanced.HorizontalPodAutoscalerConfig.Name, "the handed-over HPA keeps its name")
	assert.Equal(t, "true", after.Annotations[kedav1.ScaledObjectTransferHpaOwnershipAnnotation])
}

// TestKEDATakeover_RestartWhileWaitingKeepsTheHPA covers a controller restart
// between the ScaledObject write and KEDA's takeover: the next dispatch
// neither rewrites nor deletes the HPA and leaves the ScaledObject as is.
func TestKEDATakeover_RestartWhileWaitingKeepsTheHPA(t *testing.T) {
	seed := switchClient(t, nil, workingHPA())
	requirePending(t, DispatchAutoscaler(context.Background(), switchParams(seed, kedaClass())), v1beta1.AutoscalerSwitchedReasonWaitingForKEDA)
	hpa, so := getSwitchHPA(t, seed), getSwitchSO(t, seed)
	hpa.ResourceVersion, so.ResourceVersion = "", ""

	writes := &scalerWrites{}
	c := switchClient(t, writes, hpa, so)
	for i := 0; i < 3; i++ {
		requirePending(t, DispatchAutoscaler(context.Background(), switchParams(c, kedaClass())), v1beta1.AutoscalerSwitchedReasonWaitingForKEDA)
	}
	assert.Empty(t, writes.log)
	assert.Equal(t, dispatchOwner(switchName), *metav1.GetControllerOf(getSwitchHPA(t, c)))
}

func TestKEDATakeover_RequestedCustomNameAppliesAfterTakeover(t *testing.T) {
	const customName = "custom-keda-hpa"
	c := switchClient(t, nil, workingHPA())
	params := switchParams(c, kedaClassWithHPAName(customName))

	requirePending(t, DispatchAutoscaler(context.Background(), params), v1beta1.AutoscalerSwitchedReasonWaitingForKEDA)
	so := getSwitchSO(t, c)
	assert.Equal(t, switchName, so.Spec.Advanced.HorizontalPodAutoscalerConfig.Name, "KEDA can only take over the HPA by its own name")

	requirePending(t, DispatchAutoscaler(context.Background(), params), v1beta1.AutoscalerSwitchedReasonWaitingForKEDA)
	assert.Equal(t, switchName, getSwitchSO(t, c).Spec.Advanced.HorizontalPodAutoscalerConfig.Name, "the name holds until KEDA controls the HPA")

	simulateKEDATakeover(t, c, so, workingHPA())
	require.NoError(t, DispatchAutoscaler(context.Background(), params))
	renamed := getSwitchSO(t, c)
	assert.Equal(t, customName, renamed.Spec.Advanced.HorizontalPodAutoscalerConfig.Name, "KEDA renames its HPA once it controls it")
	assert.NotContains(t, renamed.Annotations, kedav1.ScaledObjectTransferHpaOwnershipAnnotation)
}

// TestKEDATakeover_ExistingScaledObjectKeepsItsHPAName covers services that
// already run on KEDA: OME never adds the takeover name to their
// ScaledObject, because KEDA would replace their HPA.
func TestKEDATakeover_ExistingScaledObjectKeepsItsHPAName(t *testing.T) {
	for _, tt := range []struct {
		name      string
		requested *v1beta1.ComponentAutoscaler
		wantName  string
	}{
		{name: "KEDA default name", requested: kedaClass()},
		{name: "custom name", requested: kedaClassWithHPAName("custom-keda-hpa"), wantName: "custom-keda-hpa"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			seed := switchClient(t, nil)
			require.NoError(t, DispatchAutoscaler(context.Background(), switchParams(seed, tt.requested)))
			so := getSwitchSO(t, seed)
			so.ResourceVersion = ""

			writes := &scalerWrites{}
			c := switchClient(t, writes, so)
			require.NoError(t, DispatchAutoscaler(context.Background(), switchParams(c, tt.requested)))
			assert.Empty(t, writes.log)
			got := getSwitchSO(t, c)
			var name string
			if got.Spec.Advanced != nil && got.Spec.Advanced.HorizontalPodAutoscalerConfig != nil {
				name = got.Spec.Advanced.HorizontalPodAutoscalerConfig.Name
			}
			assert.Equal(t, tt.wantName, name)
			assert.NotContains(t, got.Annotations, kedav1.ScaledObjectTransferHpaOwnershipAnnotation)
		})
	}
}

// TestKEDATakeover_StaleHPANextToLiveScaledObjectIsDeletedFirst covers an OME
// HPA left next to a ScaledObject that already scales the target: KEDA's
// webhook rejects ScaledObject writes while it exists, so OME deletes it
// before writing the ScaledObject.
func TestKEDATakeover_StaleHPANextToLiveScaledObjectIsDeletedFirst(t *testing.T) {
	seed := switchClient(t, nil)
	require.NoError(t, DispatchAutoscaler(context.Background(), switchParams(seed, kedaClass())))
	so := getSwitchSO(t, seed)
	so.ResourceVersion = ""

	writes := &scalerWrites{}
	c := switchClient(t, writes, so, workingHPA())
	changed := kedaClass()
	changed.Keda.Triggers[0].Metadata["desiredReplicas"] = "3"
	require.NoError(t, DispatchAutoscaler(context.Background(), switchParams(c, changed)))

	assert.Equal(t, []string{
		"delete HorizontalPodAutoscaler " + switchName,
		"update ScaledObject " + switchSOKey.Name,
	}, writes.log)
	assert.NotContains(t, getSwitchSO(t, c).Annotations, kedav1.ScaledObjectTransferHpaOwnershipAnnotation)
}

// TestKEDATakeover_HPAForAnotherTargetIsReplaced covers an OME HPA that scales
// a different target, such as the Deployment of the other deployment mode. It
// does not conflict with the new ScaledObject, so OME creates the ScaledObject
// first and then deletes the HPA.
func TestKEDATakeover_HPAForAnotherTargetIsReplaced(t *testing.T) {
	stale := workingHPA()
	stale.Spec.ScaleTargetRef = dispatchDeploymentTargetRef(switchName)
	writes := &scalerWrites{}
	c := switchClient(t, writes, stale)

	require.NoError(t, DispatchAutoscaler(context.Background(), switchParams(c, kedaClass())))
	assert.Equal(t, []string{
		"create ScaledObject " + switchSOKey.Name,
		"delete HorizontalPodAutoscaler " + switchName,
	}, writes.log)
	assert.NotContains(t, getSwitchSO(t, c).Annotations, kedav1.ScaledObjectTransferHpaOwnershipAnnotation)
}

func TestKEDATakeover_HPALeftByEarlierScaledObjectIsDeletedFirst(t *testing.T) {
	gone := &kedav1.ScaledObject{ObjectMeta: metav1.ObjectMeta{Namespace: switchNS, Name: switchSOKey.Name, UID: "deleted-scaledobject-uid"}}
	writes := &scalerWrites{}
	c := switchClient(t, writes, kedaCreatedHPA(switchName, gone))

	require.NoError(t, DispatchAutoscaler(context.Background(), switchParams(c, kedaClass())))
	assert.Equal(t, []string{
		"delete HorizontalPodAutoscaler " + switchName,
		"create ScaledObject " + switchSOKey.Name,
	}, writes.log)
}

func TestKEDATakeover_RejectsHPAOfAnotherScaledObject(t *testing.T) {
	other := &kedav1.ScaledObject{ObjectMeta: metav1.ObjectMeta{Namespace: switchNS, Name: "other-scaledobject", UID: "other-scaledobject-uid"}}
	writes := &scalerWrites{}
	c := switchClient(t, writes, kedaCreatedHPA(switchName, other))

	err := DispatchAutoscaler(context.Background(), switchParams(c, kedaClass()))
	require.Error(t, err)
	assert.False(t, IsTransitionPending(err))
	assert.Contains(t, err.Error(), "not controlled by expected owner")
	assert.Empty(t, writes.log)
}

// TestKEDATakeover_RejectedScaledObjectLeavesTheHPAIntact models KEDA's
// admission webhook refusing the ScaledObject.
func TestKEDATakeover_RejectedScaledObjectLeavesTheHPAIntact(t *testing.T) {
	writes := &scalerWrites{}
	rejecting := writes.funcs()
	record := rejecting.Create
	rejecting.Create = func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if _, ok := obj.(*kedav1.ScaledObject); ok {
			return apierrors.NewForbidden(schema.GroupResource{Group: "keda.sh", Resource: "scaledobjects"}, obj.GetName(),
				errors.New(`admission webhook "vscaledobject.kb.io" denied the request`))
		}
		return record(ctx, c, obj, opts...)
	}
	c := fake.NewClientBuilder().WithScheme(dispatchScheme(t)).WithObjects(workingHPA()).WithInterceptorFuncs(rejecting).Build()
	before := getSwitchHPA(t, c)

	err := DispatchAutoscaler(context.Background(), switchParams(c, kedaClass()))
	require.Error(t, err)
	assert.False(t, IsTransitionPending(err))
	assert.Contains(t, err.Error(), "denied the request")
	assert.Empty(t, writes.log)

	after := getSwitchHPA(t, c)
	assert.Equal(t, before.UID, after.UID)
	assert.Equal(t, before.ResourceVersion, after.ResourceVersion)
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), switchSOKey, &kedav1.ScaledObject{})))
}

func TestKEDATakeover_TerminatingHPAWaits(t *testing.T) {
	hpa := workingHPA()
	hpa.Finalizers = []string{"example.com/finalizer"}
	writes := &scalerWrites{}
	c := switchClient(t, writes, hpa)
	require.NoError(t, c.Delete(context.Background(), getSwitchHPA(t, c)))
	writes.log = nil

	requirePending(t, DispatchAutoscaler(context.Background(), switchParams(c, kedaClass())), v1beta1.AutoscalerSwitchedReasonWaitingForHPADeletion)
	assert.Empty(t, writes.log)
}

func TestSwitchToHPA_RemovesTakenOverScaledObjectBeforeCreatingTheHPA(t *testing.T) {
	writes := &scalerWrites{}
	c := takenOverState(t, writes)

	require.NoError(t, DispatchAutoscaler(context.Background(), switchParams(c, hpaClass())))
	assert.Equal(t, []string{
		"validate HorizontalPodAutoscaler " + switchName + "-",
		"delete ScaledObject " + switchSOKey.Name,
		"delete HorizontalPodAutoscaler " + switchName,
		"create HorizontalPodAutoscaler " + switchName,
	}, writes.log)

	hpa := getSwitchHPA(t, c)
	assert.NotEqual(t, types.UID("working-hpa-uid"), hpa.UID, "OME creates its own HPA instead of reusing KEDA's")
	assert.Equal(t, dispatchOwner(switchName), *metav1.GetControllerOf(hpa))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), switchSOKey, &kedav1.ScaledObject{})))
}

// TestSwitchToHPA_WaitsForKEDAToFinishDeleting models KEDA's finalizer on the
// ScaledObject: OME creates its HPA only after the ScaledObject is gone.
func TestSwitchToHPA_WaitsForKEDAToFinishDeleting(t *testing.T) {
	c := takenOverState(t, nil)
	so := getSwitchSO(t, c)
	so.Finalizers = []string{"finalizer.keda.sh"}
	require.NoError(t, c.Update(context.Background(), so))
	params := switchParams(c, hpaClass())

	requirePending(t, DispatchAutoscaler(context.Background(), params), v1beta1.AutoscalerSwitchedReasonWaitingForScaledObjectDeletion)
	requirePending(t, DispatchAutoscaler(context.Background(), params), v1beta1.AutoscalerSwitchedReasonWaitingForScaledObjectDeletion)
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), switchHPAKey, &autoscalingv2.HorizontalPodAutoscaler{})),
		"the HPA KEDA took over is deleted and OME has not created its own yet")

	terminating := getSwitchSO(t, c)
	require.False(t, terminating.DeletionTimestamp.IsZero())
	terminating.Finalizers = nil
	require.NoError(t, c.Update(context.Background(), terminating))

	require.NoError(t, DispatchAutoscaler(context.Background(), params))
	assert.Equal(t, dispatchOwner(switchName), *metav1.GetControllerOf(getSwitchHPA(t, c)))
}

func TestSwitchToHPA_DeletesEveryHPAKEDACreated(t *testing.T) {
	seed := switchClient(t, nil)
	require.NoError(t, DispatchAutoscaler(context.Background(), switchParams(seed, kedaClassWithHPAName("custom-keda-hpa"))))
	so := getSwitchSO(t, seed)
	so.ResourceVersion = ""
	writes := &scalerWrites{}
	c := switchClient(t, writes, so, kedaCreatedHPA("custom-keda-hpa", so), kedaCreatedHPA("keda-hpa-earlier-name", so))

	require.NoError(t, DispatchAutoscaler(context.Background(), switchParams(c, hpaClass())))
	assert.Equal(t, []string{
		"validate HorizontalPodAutoscaler " + switchName + "-",
		"delete ScaledObject " + switchSOKey.Name,
		"delete HorizontalPodAutoscaler custom-keda-hpa",
		"delete HorizontalPodAutoscaler keda-hpa-earlier-name",
		"create HorizontalPodAutoscaler " + switchName,
	}, writes.log)
}

func TestSwitchToHPA_InvalidReplacementKeepsKEDAScaling(t *testing.T) {
	writes := &scalerWrites{}
	c := takenOverState(t, writes)
	validating := writes.funcs()
	record := validating.Create
	validating.Create = func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		createOpts := &client.CreateOptions{}
		createOpts.ApplyOptions(opts)
		if _, ok := obj.(*autoscalingv2.HorizontalPodAutoscaler); ok && slices.Contains(createOpts.DryRun, metav1.DryRunAll) {
			return apierrors.NewInvalid(schema.GroupKind{Group: "autoscaling", Kind: "HorizontalPodAutoscaler"}, obj.GetGenerateName(),
				field.ErrorList{field.Invalid(field.NewPath("spec", "metrics"), nil, "must populate information for the given metric source")})
		}
		return record(ctx, cl, obj, opts...)
	}
	objs := []client.Object{getSwitchHPA(t, c), getSwitchSO(t, c)}
	for _, obj := range objs {
		obj.SetResourceVersion("")
	}
	c = fake.NewClientBuilder().WithScheme(dispatchScheme(t)).WithObjects(objs...).WithInterceptorFuncs(validating).Build()

	err := DispatchAutoscaler(context.Background(), switchParams(c, hpaClass()))
	require.Error(t, err)
	assert.False(t, IsTransitionPending(err))
	assert.Contains(t, err.Error(), "validate the replacement HPA")
	assert.Empty(t, writes.log)
	getSwitchSO(t, c)
	assert.Equal(t, types.UID("working-hpa-uid"), getSwitchHPA(t, c).UID)
}

func TestSwitchToHPA_LeftoverKEDAHPAIsDeleted(t *testing.T) {
	gone := &kedav1.ScaledObject{ObjectMeta: metav1.ObjectMeta{Namespace: switchNS, Name: switchSOKey.Name, UID: "deleted-scaledobject-uid"}}
	writes := &scalerWrites{}
	c := switchClient(t, writes, kedaCreatedHPA(switchName, gone))

	require.NoError(t, DispatchAutoscaler(context.Background(), switchParams(c, hpaClass())))
	assert.Equal(t, []string{
		"delete HorizontalPodAutoscaler " + switchName,
		"create HorizontalPodAutoscaler " + switchName,
	}, writes.log)
	assert.Equal(t, dispatchOwner(switchName), *metav1.GetControllerOf(getSwitchHPA(t, c)))
}

func TestSwitchToHPA_TerminatingKEDAHPAWaits(t *testing.T) {
	gone := &kedav1.ScaledObject{ObjectMeta: metav1.ObjectMeta{Namespace: switchNS, Name: switchSOKey.Name, UID: "deleted-scaledobject-uid"}}
	leftover := kedaCreatedHPA("custom-keda-hpa", gone)
	leftover.Finalizers = []string{"example.com/finalizer"}
	c := switchClient(t, nil, leftover)
	require.NoError(t, c.Delete(context.Background(), leftover.DeepCopy()))

	requirePending(t, DispatchAutoscaler(context.Background(), switchParams(c, hpaClass())), v1beta1.AutoscalerSwitchedReasonWaitingForHPADeletion)
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), switchHPAKey, &autoscalingv2.HorizontalPodAutoscaler{})),
		"OME's HPA waits until the KEDA HPA is gone")
}

// TestSwitchToHPA_WorkingHPANextToScaledObjectNeedsNoValidation covers an OME
// HPA that already runs next to the ScaledObject: removing the ScaledObject
// leaves a working scaler, so no dry run is needed.
func TestSwitchToHPA_WorkingHPANextToScaledObjectNeedsNoValidation(t *testing.T) {
	seed := switchClient(t, nil)
	require.NoError(t, DispatchAutoscaler(context.Background(), switchParams(seed, kedaClass())))
	so := getSwitchSO(t, seed)
	so.ResourceVersion = ""
	writes := &scalerWrites{}
	c := switchClient(t, writes, so, workingHPA())

	require.NoError(t, DispatchAutoscaler(context.Background(), switchParams(c, hpaClass())))
	require.NotEmpty(t, writes.log)
	assert.Equal(t, "delete ScaledObject "+switchSOKey.Name, writes.log[0])
	assert.NotContains(t, writes.log, "validate HorizontalPodAutoscaler "+switchName+"-")
	assert.Equal(t, types.UID("working-hpa-uid"), getSwitchHPA(t, c).UID)
}

func TestSwitchToHPA_RejectsHPAOfAnotherScaledObject(t *testing.T) {
	other := &kedav1.ScaledObject{ObjectMeta: metav1.ObjectMeta{Namespace: switchNS, Name: "other-scaledobject", UID: "other-scaledobject-uid"}}
	writes := &scalerWrites{}
	c := switchClient(t, writes, kedaCreatedHPA(switchName, other))

	err := DispatchAutoscaler(context.Background(), switchParams(c, hpaClass()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not controlled by expected owner")
	assert.Empty(t, writes.log)
}

func TestSwitchToNone_LeavesTakenOverHPAToTheGarbageCollector(t *testing.T) {
	writes := &scalerWrites{}
	c := takenOverState(t, writes)

	require.NoError(t, DispatchAutoscaler(context.Background(), switchParams(c, &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone})))
	assert.Equal(t, []string{"delete ScaledObject " + switchSOKey.Name}, writes.log,
		"the HPA belongs to the ScaledObject; deleting the ScaledObject removes it")
}

func TestPlanScaledObjectHPA(t *testing.T) {
	liveWith := func(name string) *kedav1.ScaledObject {
		so := &kedav1.ScaledObject{ObjectMeta: metav1.ObjectMeta{UID: "live-scaledobject-uid"}}
		if name != "" {
			so.Spec.Advanced = &kedav1.AdvancedConfig{HorizontalPodAutoscalerConfig: &kedav1.HorizontalPodAutoscalerConfig{Name: name}}
		}
		return so
	}
	tests := []struct {
		name      string
		requested *v1beta1.ComponentAutoscaler
		liveSO    *kedav1.ScaledObject
		hpa       componentHPA
		want      scaledObjectHPAPlan
	}{
		{
			name:      "new ScaledObject, no HPA",
			requested: kedaClass(),
			want:      scaledObjectHPAPlan{},
		},
		{
			name:      "new ScaledObject takes over the working HPA",
			requested: kedaClass(),
			hpa:       componentHPA{control: hpaControlledByWorkload, targetMatches: true},
			want:      scaledObjectHPAPlan{hpaName: switchName, takeOver: true},
		},
		{
			name:      "new ScaledObject with a custom name still takes over first",
			requested: kedaClassWithHPAName("custom"),
			hpa:       componentHPA{control: hpaControlledByWorkload, targetMatches: true},
			want:      scaledObjectHPAPlan{hpaName: switchName, takeOver: true},
		},
		{
			name:      "new ScaledObject replaces an HPA for another target",
			requested: kedaClass(),
			hpa:       componentHPA{control: hpaControlledByWorkload},
			want:      scaledObjectHPAPlan{deleteAfterWrite: true},
		},
		{
			name:      "new ScaledObject naming the Component's HPA takes over an HPA for another target",
			requested: kedaClassWithHPAName(switchName),
			hpa:       componentHPA{control: hpaControlledByWorkload},
			want:      scaledObjectHPAPlan{hpaName: switchName, takeOver: true},
		},
		{
			name:      "new ScaledObject deletes an HPA left by an earlier ScaledObject",
			requested: kedaClass(),
			hpa:       componentHPA{control: hpaLeftByScaledObject, targetMatches: true},
			want:      scaledObjectHPAPlan{deleteBeforeWrite: true},
		},
		{
			name:      "taking over: name kept while the workload controls the HPA",
			requested: kedaClassWithHPAName("custom"),
			liveSO:    liveWith(switchName),
			hpa:       componentHPA{control: hpaControlledByWorkload, targetMatches: true},
			want:      scaledObjectHPAPlan{hpaName: switchName, takeOver: true},
		},
		{
			name:      "taken over: name kept without a requested name",
			requested: kedaClass(),
			liveSO:    liveWith(switchName),
			hpa:       componentHPA{control: hpaControlledByScaledObject, targetMatches: true},
			want:      scaledObjectHPAPlan{hpaName: switchName, takeOver: true},
		},
		{
			name:      "taken over: a requested name renames the HPA",
			requested: kedaClassWithHPAName("custom"),
			liveSO:    liveWith(switchName),
			hpa:       componentHPA{control: hpaControlledByScaledObject, targetMatches: true},
			want:      scaledObjectHPAPlan{hpaName: "custom"},
		},
		{
			name:      "live ScaledObject on KEDA's default name stays there",
			requested: kedaClass(),
			liveSO:    liveWith(""),
			want:      scaledObjectHPAPlan{},
		},
		{
			name:      "live ScaledObject on KEDA's default name deletes a stale HPA first",
			requested: kedaClass(),
			liveSO:    liveWith(""),
			hpa:       componentHPA{control: hpaControlledByWorkload, targetMatches: true},
			want:      scaledObjectHPAPlan{deleteBeforeWrite: true},
		},
		{
			name:      "live ScaledObject leaves a renamed HPA to KEDA",
			requested: kedaClassWithHPAName("custom"),
			liveSO:    liveWith("custom"),
			hpa:       componentHPA{control: hpaControlledByScaledObject, targetMatches: true},
			want:      scaledObjectHPAPlan{hpaName: "custom"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := switchParams(fake.NewClientBuilder().WithScheme(dispatchScheme(t)).Build(), tt.requested)
			assert.Equal(t, tt.want, planScaledObjectHPA(p, tt.liveSO, tt.hpa))
		})
	}
}

// longSwitchName is a Component name for which KEDA's own HPA name,
// "keda-hpa-scaledobject-" plus the Component name, is 64 characters: one
// more than KEDA's webhook accepts.
const longSwitchName = "team-a-example-model-shared-shadow-decoder"

func longSwitchParams(c client.Client, autoscaler *v1beta1.ComponentAutoscaler) DispatchParams {
	p := switchParams(c, autoscaler)
	p.Name = longSwitchName
	p.Owner = dispatchOwner(longSwitchName)
	p.ScaleTargetRef = dispatchScaleTargetRef(longSwitchName)
	return p
}

func TestPlanScaledObjectHPA_LongComponentName(t *testing.T) {
	shortened := keda.ShortenedHPAName(utils.GetScaledObjectName(longSwitchName))
	require.NotEmpty(t, shortened, "test setup: KEDA's own HPA name must be too long")
	liveWith := func(name string) *kedav1.ScaledObject {
		so := &kedav1.ScaledObject{ObjectMeta: metav1.ObjectMeta{UID: "live-scaledobject-uid"}}
		if name != "" {
			so.Spec.Advanced = &kedav1.AdvancedConfig{HorizontalPodAutoscalerConfig: &kedav1.HorizontalPodAutoscalerConfig{Name: name}}
		}
		return so
	}
	tests := []struct {
		name      string
		requested *v1beta1.ComponentAutoscaler
		liveSO    *kedav1.ScaledObject
		hpa       componentHPA
		want      scaledObjectHPAPlan
	}{
		{
			name:      "new ScaledObject, no HPA, gets the shortened name",
			requested: kedaClass(),
			want:      scaledObjectHPAPlan{hpaName: shortened},
		},
		{
			name:      "new ScaledObject replacing an HPA for another target gets the shortened name",
			requested: kedaClass(),
			hpa:       componentHPA{control: hpaControlledByWorkload},
			want:      scaledObjectHPAPlan{hpaName: shortened, deleteAfterWrite: true},
		},
		{
			name:      "new ScaledObject after an earlier ScaledObject gets the shortened name",
			requested: kedaClass(),
			hpa:       componentHPA{control: hpaLeftByScaledObject, targetMatches: true},
			want:      scaledObjectHPAPlan{hpaName: shortened, deleteBeforeWrite: true},
		},
		{
			name:      "new ScaledObject still takes over the working HPA by the Component name",
			requested: kedaClass(),
			hpa:       componentHPA{control: hpaControlledByWorkload, targetMatches: true},
			want:      scaledObjectHPAPlan{hpaName: longSwitchName, takeOver: true},
		},
		{
			name:      "a requested name wins over the shortened name",
			requested: kedaClassWithHPAName("custom"),
			want:      scaledObjectHPAPlan{hpaName: "custom"},
		},
		{
			name:      "live ScaledObject keeps the shortened name",
			requested: kedaClass(),
			liveSO:    liveWith(shortened),
			hpa:       componentHPA{},
			want:      scaledObjectHPAPlan{hpaName: shortened},
		},
		{
			name:      "dropping a requested name moves to the shortened name",
			requested: kedaClass(),
			liveSO:    liveWith("custom"),
			want:      scaledObjectHPAPlan{hpaName: shortened},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := longSwitchParams(fake.NewClientBuilder().WithScheme(dispatchScheme(t)).Build(), tt.requested)
			assert.Equal(t, tt.want, planScaledObjectHPA(p, tt.liveSO, tt.hpa))
		})
	}
}

func TestDispatchKEDA_LongComponentNameGetsAShortHPAName(t *testing.T) {
	writes := &scalerWrites{}
	c := switchClient(t, writes)
	soName := utils.GetScaledObjectName(longSwitchName)

	require.NoError(t, DispatchAutoscaler(context.Background(), longSwitchParams(c, kedaClass())))
	require.NoError(t, DispatchAutoscaler(context.Background(), longSwitchParams(c, kedaClass())))

	assert.Equal(t, []string{"create ScaledObject " + soName}, writes.log, "the second pass writes nothing")
	so := &kedav1.ScaledObject{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: switchNS, Name: soName}, so))
	hpaName := keda.ConfiguredHPAName(so.Spec.Advanced)
	assert.Equal(t, keda.ShortenedHPAName(soName), hpaName)
	assert.Empty(t, validation.IsDNS1123Label(hpaName), "KEDA's webhook accepts only HPA names that are DNS labels")
	assert.NotContains(t, so.Annotations, kedav1.ScaledObjectTransferHpaOwnershipAnnotation, "there is no HPA to take over")
}

func TestSameScaleTarget(t *testing.T) {
	ir := dispatchScaleTargetRef(switchName)
	otherVersion := ir
	otherVersion.APIVersion = v1beta1.SchemeGroupVersion.Group + "/v2"
	assert.True(t, sameScaleTarget(ir, ir))
	assert.True(t, sameScaleTarget(ir, otherVersion), "the version does not change the target")
	assert.False(t, sameScaleTarget(ir, dispatchDeploymentTargetRef(switchName)))
	otherName := ir
	otherName.Name = "other"
	assert.False(t, sameScaleTarget(ir, otherName))
}
