package autoscaler

import (
	"context"
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// rawHoldScheme extends the dispatch scheme with appsv1: the hold path
// reads the last-known-good record off the component's Deployment.
func rawHoldScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := dispatchScheme(t)
	require.NoError(t, appsv1.AddToScheme(scheme))
	return scheme
}

// rawHoldDeployment materializes the component Deployment carrying the
// encoded last-known-good policy render.
func rawHoldDeployment(t *testing.T, namespace, name string, lkg *v1beta1.ComponentAutoscaler) *appsv1.Deployment {
	t.Helper()
	encoded, err := MarshalLastRendered(lkg)
	require.NoError(t, err)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   namespace,
			Name:        name,
			Annotations: map[string]string{constants.AutoscalerPolicyLastRendered: encoded},
		},
	}
}

// TestDispatchForRawComponent_HoldRedispatchesLastRendered pins the hold
// recovery path: the ScaledObject was deleted externally, but the
// Deployment still carries the last-known-good record — a hold pass must
// re-create the SO from that record so bounds keep flowing to a live
// scaler through the freeze.
func TestDispatchForRawComponent_HoldRedispatchesLastRendered(t *testing.T) {
	componentMeta := rawDispatchComponentMeta()
	lkg := rawDispatchKEDAAutoscaler()
	scheme := rawHoldScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(rawHoldDeployment(t, componentMeta.Namespace, componentMeta.Name, lkg)).
		Build()

	require.NoError(t, DispatchForRawComponent(context.Background(), RawDispatchInput{
		Client:             cl,
		Scheme:             scheme,
		ISVC:               rawDispatchISVC(),
		ComponentMeta:      componentMeta,
		ResolvedAutoscaler: nil,
		ComponentExt:       rawDispatchComponentExt(),
		PolicyHold:         true,
	}))

	so := &kedav1.ScaledObject{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
		Namespace: componentMeta.Namespace,
		Name:      utils.GetScaledObjectName(componentMeta.Name),
	}, so), "the hold must re-create the ScaledObject from the record")
	assert.Equal(t, lkg.Keda.Triggers, so.Spec.Triggers,
		"trigger content comes from the last-known-good record, not the (nil) pass block")
	require.NotNil(t, so.Spec.ScaleTargetRef)
	assert.Equal(t, "Deployment", so.Spec.ScaleTargetRef.Kind)
	assert.Equal(t, componentMeta.Name, so.Spec.ScaleTargetRef.Name)
	require.NotNil(t, so.Spec.MinReplicaCount)
	assert.Equal(t, int32(2), *so.Spec.MinReplicaCount,
		"bounds keep flowing from the component spec during the freeze")
	require.NotNil(t, so.Spec.MaxReplicaCount)
	assert.Equal(t, int32(7), *so.Spec.MaxReplicaCount)
	assertRawAutoscalerOwner(t, so.OwnerReferences)
}

// TestDispatchForRawComponent_HoldWithoutRecordDoesNothing pins the
// no-record hold: with no Deployment (hence no last-known-good) the
// dispatch must do nothing at all — no ScaledObject appears, no default
// HPA substitutes the broken policy, and objects that would fall to the
// nil-block delete branch on an ordinary pass survive untouched.
func TestDispatchForRawComponent_HoldWithoutRecordDoesNothing(t *testing.T) {
	componentMeta := rawDispatchComponentMeta()
	scheme := rawHoldScheme(t)
	// The pre-ref scaler (ISVC-owned HPA) and a foreign SO both live at the
	// canonical keys; an ordinary nil-block dispatch would delete the owned
	// HPA, so their survival proves the hold short-circuits before dispatch.
	ownedHPA := existingHPA(componentMeta.Namespace, componentMeta.Name, rawDispatchOwner())
	foreignSO := existingScaledObject(componentMeta.Namespace, componentMeta.Name, foreignDispatchOwner(componentMeta.Name))
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ownedHPA, foreignSO).Build()

	require.NoError(t, DispatchForRawComponent(context.Background(), RawDispatchInput{
		Client:             cl,
		Scheme:             scheme,
		ISVC:               rawDispatchISVC(),
		ComponentMeta:      componentMeta,
		ResolvedAutoscaler: nil,
		ComponentExt:       rawDispatchComponentExt(),
		PolicyHold:         true,
	}))

	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
		Namespace: componentMeta.Namespace,
		Name:      componentMeta.Name,
	}, hpa), "a hold without a record must not tear down the existing scaler")

	so := &kedav1.ScaledObject{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
		Namespace: componentMeta.Namespace,
		Name:      utils.GetScaledObjectName(componentMeta.Name),
	}, so), "foreign objects must be untouched by a no-record hold")
	assert.Equal(t, foreignSO.Spec.Triggers, so.Spec.Triggers,
		"the foreign ScaledObject must not be rewritten")
}

// TestDispatchForRawComponent_HoldDoesNotAdvanceLastRendered pins that a
// hold never overwrites the record it is standing on: the re-dispatched
// block is the record itself, and PolicySourced stamping is reserved for
// fresh renders.
func TestDispatchForRawComponent_HoldDoesNotAdvanceLastRendered(t *testing.T) {
	componentMeta := rawDispatchComponentMeta()
	lkg := rawDispatchKEDAAutoscaler()
	scheme := rawHoldScheme(t)
	deployment := rawHoldDeployment(t, componentMeta.Namespace, componentMeta.Name, lkg)
	original := deployment.Annotations[constants.AutoscalerPolicyLastRendered]
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deployment).Build()

	require.NoError(t, DispatchForRawComponent(context.Background(), RawDispatchInput{
		Client:             cl,
		Scheme:             scheme,
		ISVC:               rawDispatchISVC(),
		ComponentMeta:      componentMeta,
		ResolvedAutoscaler: nil,
		ComponentExt:       rawDispatchComponentExt(),
		PolicySourced:      true,
		PolicyHold:         true,
	}))

	stored := &appsv1.Deployment{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
		Namespace: componentMeta.Namespace,
		Name:      componentMeta.Name,
	}, stored))
	assert.Equal(t, original, stored.Annotations[constants.AutoscalerPolicyLastRendered],
		"the record must be byte-identical after a hold pass")
}

// TestDispatchForRawComponent_HoldWithMissingDeploymentIsNoError pins the
// missing-Deployment read: a hold before the Deployment exists reads as
// "no record" and dispatches nothing rather than failing the reconcile.
func TestDispatchForRawComponent_HoldWithMissingDeploymentIsNoError(t *testing.T) {
	componentMeta := rawDispatchComponentMeta()
	scheme := rawHoldScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	require.NoError(t, DispatchForRawComponent(context.Background(), RawDispatchInput{
		Client:             cl,
		Scheme:             scheme,
		ISVC:               rawDispatchISVC(),
		ComponentMeta:      componentMeta,
		ResolvedAutoscaler: nil,
		ComponentExt:       rawDispatchComponentExt(),
		PolicyHold:         true,
	}))

	so := &kedav1.ScaledObject{}
	err := cl.Get(context.Background(), types.NamespacedName{
		Namespace: componentMeta.Namespace,
		Name:      utils.GetScaledObjectName(componentMeta.Name),
	}, so)
	assert.True(t, apierrors.IsNotFound(err), "no ScaledObject may appear without a record, got %v", err)
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	err = cl.Get(context.Background(), types.NamespacedName{
		Namespace: componentMeta.Namespace,
		Name:      componentMeta.Name,
	}, hpa)
	assert.True(t, apierrors.IsNotFound(err), "no default HPA may substitute a held policy, got %v", err)
}

// rawLastRenderedAnnotation returns the last-rendered annotation on the
// component Deployment and whether it is present.
func rawLastRenderedAnnotation(t *testing.T, cl client.Client, componentMeta metav1.ObjectMeta) (string, bool) {
	t.Helper()
	stored := &appsv1.Deployment{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
		Namespace: componentMeta.Namespace,
		Name:      componentMeta.Name,
	}, stored))
	value, ok := stored.Annotations[constants.AutoscalerPolicyLastRendered]
	return value, ok
}

// TestDispatchForRawComponent_PassWithoutPolicyDropsLastRendered checks that the
// record only describes a scaler a policy rendered: an inline, default or
// absent autoscaler removes it, and a later pass writes nothing more.
func TestDispatchForRawComponent_PassWithoutPolicyDropsLastRendered(t *testing.T) {
	cases := map[string]*v1beta1.ComponentAutoscaler{
		"inline hpa":    rawDispatchHPAAutoscaler(),
		"inline keda":   rawDispatchKEDAAutoscaler(),
		"external":      {Class: v1beta1.AutoscalerExternal},
		"no autoscaler": nil,
	}
	for name, block := range cases {
		t.Run(name, func(t *testing.T) {
			componentMeta := rawDispatchComponentMeta()
			scheme := rawHoldScheme(t)
			deployment := rawHoldDeployment(t, componentMeta.Namespace, componentMeta.Name, rawDispatchKEDAAutoscaler())
			deployment.Annotations["unrelated"] = "kept"
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deployment).Build()

			input := RawDispatchInput{
				Client:             cl,
				Scheme:             scheme,
				ISVC:               rawDispatchISVC(),
				ComponentMeta:      componentMeta,
				ResolvedAutoscaler: block,
				ComponentExt:       rawDispatchComponentExt(),
			}
			deploymentKey := types.NamespacedName{Namespace: componentMeta.Namespace, Name: componentMeta.Name}
			dispatch := func() *appsv1.Deployment {
				t.Helper()
				if err := DispatchForRawComponent(context.Background(), input); err != nil {
					require.True(t, IsTransitionPending(err), "unexpected dispatch error: %v", err)
				}
				stored := &appsv1.Deployment{}
				require.NoError(t, cl.Get(context.Background(), deploymentKey, stored))
				return stored
			}

			first := dispatch()
			_, ok := first.Annotations[constants.AutoscalerPolicyLastRendered]
			assert.False(t, ok, "a pass whose scaler no policy rendered must drop the record")
			assert.Equal(t, "kept", first.Annotations["unrelated"])

			second := dispatch()
			assert.Equal(t, first.ResourceVersion, second.ResourceVersion, "a pass with no record to drop leaves the Deployment untouched")
		})
	}
}

// TestDispatchForRawComponent_HoldAfterPolicyRefRemovedKeepsRunningHPA covers
// policy A rendering a ScaledObject, the operator removing the ref so OME runs
// its default HPA, and then a ref to a missing policy B: the hold keeps the
// running HPA and never brings back policy A's ScaledObject.
func TestDispatchForRawComponent_HoldAfterPolicyRefRemovedKeepsRunningHPA(t *testing.T) {
	ctx := context.Background()
	componentMeta := rawDispatchComponentMeta()
	scheme := rawHoldScheme(t)
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: componentMeta.Namespace, Name: componentMeta.Name}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deployment).Build()
	base := RawDispatchInput{
		Client:        cl,
		Scheme:        scheme,
		ISVC:          rawDispatchISVC(),
		ComponentMeta: componentMeta,
		ComponentExt:  rawDispatchComponentExt(),
	}
	dispatch := func(input RawDispatchInput) {
		t.Helper()
		if err := DispatchForRawComponent(ctx, input); err != nil {
			require.True(t, IsTransitionPending(err), "unexpected dispatch error: %v", err)
		}
	}

	policyA := base
	policyA.ResolvedAutoscaler = rawDispatchKEDAAutoscaler()
	policyA.PolicySourced = true
	dispatch(policyA)
	_, ok := rawLastRenderedAnnotation(t, cl, componentMeta)
	require.True(t, ok, "a policy render is recorded")

	refRemoved := base
	refRemoved.ResolvedAutoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}
	for i := 0; i < 3; i++ {
		dispatch(refRemoved)
	}
	soKey := types.NamespacedName{Namespace: componentMeta.Namespace, Name: utils.GetScaledObjectName(componentMeta.Name)}
	require.True(t, apierrors.IsNotFound(cl.Get(ctx, soKey, &kedav1.ScaledObject{})), "the default HPA replaced policy A's ScaledObject")
	hpaKey := types.NamespacedName{Namespace: componentMeta.Namespace, Name: componentMeta.Name}
	before := &autoscalingv2.HorizontalPodAutoscaler{}
	require.NoError(t, cl.Get(ctx, hpaKey, before))

	hold := base
	hold.PolicyHold = true
	for i := 0; i < 3; i++ {
		dispatch(hold)
	}
	assert.True(t, apierrors.IsNotFound(cl.Get(ctx, soKey, &kedav1.ScaledObject{})),
		"a hold on a new broken ref must not bring back policy A's ScaledObject")
	after := &autoscalingv2.HorizontalPodAutoscaler{}
	require.NoError(t, cl.Get(ctx, hpaKey, after), "the running HPA keeps scaling through the hold")
	assert.Equal(t, before.ResourceVersion, after.ResourceVersion, "the hold leaves the running HPA untouched")
}

// TestDispatchForRawComponent_HoldDuringSwitchToHPAKeepsPolicyScaledObject
// covers policy A rendering a ScaledObject, the operator removing the ref while
// KEDA still holds the ScaledObject, and then a ref to a missing policy B. The
// HPA was never created, so the hold must keep policy A's ScaledObject rather
// than leave the Deployment without any scaler.
func TestDispatchForRawComponent_HoldDuringSwitchToHPAKeepsPolicyScaledObject(t *testing.T) {
	ctx := context.Background()
	componentMeta := rawDispatchComponentMeta()
	scheme := rawHoldScheme(t)
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: componentMeta.Namespace, Name: componentMeta.Name}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deployment).Build()
	base := RawDispatchInput{
		Client:        cl,
		Scheme:        scheme,
		ISVC:          rawDispatchISVC(),
		ComponentMeta: componentMeta,
		ComponentExt:  rawDispatchComponentExt(),
	}
	soKey := types.NamespacedName{Namespace: componentMeta.Namespace, Name: utils.GetScaledObjectName(componentMeta.Name)}
	hpaKey := types.NamespacedName{Namespace: componentMeta.Namespace, Name: componentMeta.Name}

	policyA := base
	policyA.ResolvedAutoscaler = rawDispatchKEDAAutoscaler()
	policyA.PolicySourced = true
	require.NoError(t, DispatchForRawComponent(ctx, policyA))

	// KEDA's finalizer keeps the ScaledObject until KEDA finishes deleting it.
	so := &kedav1.ScaledObject{}
	require.NoError(t, cl.Get(ctx, soKey, so))
	so.Finalizers = []string{"finalizer.keda.sh"}
	require.NoError(t, cl.Update(ctx, so))

	refRemoved := base
	refRemoved.ResolvedAutoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}
	err := DispatchForRawComponent(ctx, refRemoved)
	require.True(t, IsTransitionPending(err), "the switch to the HPA waits for KEDA, got %v", err)
	require.True(t, apierrors.IsNotFound(cl.Get(ctx, hpaKey, &autoscalingv2.HorizontalPodAutoscaler{})), "no HPA exists while KEDA deletes the ScaledObject")
	_, ok := rawLastRenderedAnnotation(t, cl, componentMeta)
	assert.True(t, ok, "a switch that has not put the HPA in place keeps the policy record")

	hold := base
	hold.PolicyHold = true
	err = DispatchForRawComponent(ctx, hold)
	require.True(t, err == nil || IsTransitionPending(err), "unexpected hold error: %v", err)

	terminating := &kedav1.ScaledObject{}
	require.NoError(t, cl.Get(ctx, soKey, terminating))
	terminating.Finalizers = nil
	require.NoError(t, cl.Update(ctx, terminating))
	require.True(t, apierrors.IsNotFound(cl.Get(ctx, soKey, &kedav1.ScaledObject{})), "KEDA finished deleting the old ScaledObject")

	for i := 0; i < 3; i++ {
		err = DispatchForRawComponent(ctx, hold)
		require.True(t, err == nil || IsTransitionPending(err), "unexpected hold error: %v", err)
	}
	restored := &kedav1.ScaledObject{}
	require.NoError(t, cl.Get(ctx, soKey, restored), "the hold restores policy A's ScaledObject so the Deployment keeps a scaler")
	assert.Equal(t, rawDispatchKEDAAutoscaler().Keda.Triggers, restored.Spec.Triggers)
	assert.True(t, apierrors.IsNotFound(cl.Get(ctx, hpaKey, &autoscalingv2.HorizontalPodAutoscaler{})), "the hold creates no default HPA")
}
