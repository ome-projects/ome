package autoscaler

import (
	"context"
	"errors"
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// dispatchScheme registers autoscalingv2 + kedav1 so the fake client can
// store both kinds. Mirrors the manager-startup scheme (KEDA is a
// hard dependency; always registered).
func dispatchScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, autoscalingv2.AddToScheme(scheme))
	require.NoError(t, kedav1.AddToScheme(scheme))
	require.NoError(t, v1beta1.AddToScheme(scheme))
	return scheme
}

// dispatchOwner is the canonical owner-ref the dispatch test fixtures
// stamp on emitted HPAs / SOs. The IR-managed path supplies the live
// InferenceReplica's owner-ref; here we synthesize a stable one so
// assertions on OwnerReferences are deterministic.
func dispatchOwner(name string) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(),
		Kind:       "InferenceReplica",
		Name:       name,
		UID:        "ir-uid-12345",
		Controller: ptr.To(true),
	}
}

func dispatchISVCOwner(name string) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(),
		Kind:       "InferenceService",
		Name:       name,
		UID:        "isvc-uid-12345",
		Controller: ptr.To(true),
	}
}

func dispatchManagementLabels(isvcName string, component v1beta1.ComponentType) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: isvcName,
		constants.OMEComponentLabel:           string(component),
	}
}

func dispatchModeBridgeIR(namespace, isvcName, name string) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			UID:             dispatchOwner(name).UID,
			Labels:          dispatchManagementLabels(isvcName, v1beta1.EngineComponent),
			OwnerReferences: []metav1.OwnerReference{dispatchISVCOwner(isvcName)},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: &v1beta1.ParentReference{Name: isvcName},
			Component: v1beta1.EngineComponent,
		},
	}
}

func dispatchDeploymentTargetRef(name string) autoscalingv2.CrossVersionObjectReference {
	return autoscalingv2.CrossVersionObjectReference{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Name:       name,
	}
}

func foreignDispatchOwner(name string) metav1.OwnerReference {
	owner := dispatchOwner(name)
	owner.Name = "other-owner"
	owner.UID = "other-owner-uid"
	return owner
}

func nonControllerDispatchOwner(name string) metav1.OwnerReference {
	owner := dispatchOwner(name)
	owner.Controller = nil
	return owner
}

// dispatchScaleTargetRef builds the autoscalingv2 ref the IR-managed
// path supplies on every dispatch. Both HPA + KEDA branches pass this
// through to their generators verbatim.
func dispatchScaleTargetRef(name string) autoscalingv2.CrossVersionObjectReference {
	return autoscalingv2.CrossVersionObjectReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(),
		Kind:       "InferenceReplica",
		Name:       name,
	}
}

// existingHPA materializes a stand-in HPA at the canonical key for a
// given Component (Namespace, Name) so cross-class delete tests can
// assert it gets removed.
func existingHPA(namespace, name string, owners ...metav1.OwnerReference) *autoscalingv2.HorizontalPodAutoscaler {
	maxR := int32(3)
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, OwnerReferences: owners},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: dispatchScaleTargetRef(name),
			MaxReplicas:    maxR,
		},
	}
}

// existingScaledObject materializes a stand-in ScaledObject at the
// canonical KEDA key (utils.GetScaledObjectName(name)) for cross-class
// delete tests.
func existingScaledObject(namespace, name string, owners ...metav1.OwnerReference) *kedav1.ScaledObject {
	maxR := int32(3)
	return &kedav1.ScaledObject{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       namespace,
			Name:            utils.GetScaledObjectName(name),
			OwnerReferences: owners,
		},
		Spec: kedav1.ScaledObjectSpec{
			ScaleTargetRef: &kedav1.ScaleTarget{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceReplica",
				Name:       name,
			},
			MaxReplicaCount: &maxR,
			Triggers: []kedav1.ScaleTriggers{
				{Type: "cron", Metadata: map[string]string{"timezone": "UTC", "start": "0 0 * * *", "end": "0 12 * * *", "desiredReplicas": "2"}},
			},
		},
	}
}

// kedaAutoscalerBlock builds a minimal KedaAutoscaler with one trigger
// — the KEDA generator's shouldCreateScaledObject gate rejects a zero-
// trigger SO, so every "create SO" case must supply at least one.
func kedaAutoscalerBlock() *v1beta1.KedaAutoscaler {
	return &v1beta1.KedaAutoscaler{
		Triggers: []kedav1.ScaleTriggers{
			{Type: "cron", Metadata: map[string]string{"timezone": "UTC", "start": "0 9 * * *", "end": "0 17 * * *", "desiredReplicas": "2"}},
		},
	}
}

// TestDispatchAutoscaler_Branches pins every Class branch — hpa, keda,
// external, none — both in the "no pre-existing autoscaler" steady state
// and with a pre-existing OME-managed sibling that must be deleted. This
// is the load-bearing test for the twin rule (none + external are status-field
// twins, share dispatch) and the always-on rule (even when class=none we
// still run the reconcile to ensure no stale OME-managed object lingers).
func TestDispatchAutoscaler_Branches(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)

	cases := []struct {
		name string
		// autoscaler is the resolved ComponentAutoscaler the dispatch
		// reads off Params. nil exercises the "treat as none" branch.
		autoscaler *v1beta1.ComponentAutoscaler
		// preexisting describes any OME-managed sibling planted in the
		// fake client before the dispatch runs. Tests cross-class
		// delete + the steady-state "no sibling" branch.
		preExistHPA bool
		preExistSO  bool
		// Post-conditions asserted after a single DispatchAutoscaler.
		wantHPAExists bool
		wantSOExists  bool
	}{
		{
			name:          "class=hpa, no preexisting → HPA created, no SO",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			wantHPAExists: true,
			wantSOExists:  false,
		},
		{
			name:          "class=hpa, preexisting SO → SO deleted + HPA created (cross-class switch keda→hpa)",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			preExistSO:    true,
			wantHPAExists: true,
			wantSOExists:  false,
		},
		{
			name:          "class=keda with one trigger, no preexisting → SO created, no HPA",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
			wantHPAExists: false,
			wantSOExists:  true,
		},
		{
			name:          "class=keda with one trigger, preexisting HPA → HPA deleted + SO created (cross-class switch hpa→keda)",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
			preExistHPA:   true,
			wantHPAExists: false,
			wantSOExists:  true,
		},
		{
			name:          "class=external, preexisting HPA → HPA deleted, no SO (twin)",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal},
			preExistHPA:   true,
			wantHPAExists: false,
			wantSOExists:  false,
		},
		{
			name:          "class=external, preexisting SO → SO deleted, no HPA (twin)",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal},
			preExistSO:    true,
			wantHPAExists: false,
			wantSOExists:  false,
		},
		{
			name:          "class=external, preexisting both → both deleted",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal},
			preExistHPA:   true,
			preExistSO:    true,
			wantHPAExists: false,
			wantSOExists:  false,
		},
		{
			name:          "class=none, preexisting both → both deleted (behaves identically to external)",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
			preExistHPA:   true,
			preExistSO:    true,
			wantHPAExists: false,
			wantSOExists:  false,
		},
		{
			name:          "Autoscaler == nil, preexisting both → both deleted (treat-as-none)",
			autoscaler:    nil,
			preExistHPA:   true,
			preExistSO:    true,
			wantHPAExists: false,
			wantSOExists:  false,
		},
		{
			name:          "class=none, no preexisting → no-op (idempotent on empty state)",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
			wantHPAExists: false,
			wantSOExists:  false,
		},
		{
			name:          "class=hpa, preexisting HPA → idempotent re-reconcile (no spurious delete)",
			autoscaler:    &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			preExistHPA:   true,
			wantHPAExists: true,
			wantSOExists:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := dispatchScheme(t)
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tc.preExistHPA {
				builder = builder.WithObjects(existingHPA(ns, name, dispatchOwner(name)))
			}
			if tc.preExistSO {
				builder = builder.WithObjects(existingScaledObject(ns, name, dispatchOwner(name)))
			}
			cl := builder.Build()

			err := DispatchAutoscaler(context.Background(), DispatchParams{
				Client:         cl,
				Scheme:         scheme,
				Owner:          dispatchOwner(name),
				Namespace:      ns,
				Name:           name,
				ScaleTargetRef: dispatchScaleTargetRef(name),
				Autoscaler:     tc.autoscaler,
				MinReplicas:    1,
				MaxReplicas:    5,
			})
			require.NoError(t, err)

			// HPA existence assertion.
			gotHPA := &autoscalingv2.HorizontalPodAutoscaler{}
			hpaErr := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, gotHPA)
			if tc.wantHPAExists {
				require.NoError(t, hpaErr, "expected HPA to exist")
				assert.Equal(t, dispatchScaleTargetRef(name), gotHPA.Spec.ScaleTargetRef, "HPA scaleTargetRef should target the IR")
				// Owner-ref should pin the HPA to the IR (GC cascade).
				require.Len(t, gotHPA.OwnerReferences, 1, "HPA should have exactly one ownerRef")
				assert.Equal(t, "InferenceReplica", gotHPA.OwnerReferences[0].Kind)
				assert.Equal(t, name, gotHPA.OwnerReferences[0].Name)
			} else {
				assert.True(t, apierrors.IsNotFound(hpaErr), "expected HPA to be absent, got err=%v", hpaErr)
			}

			// ScaledObject existence assertion.
			gotSO := &kedav1.ScaledObject{}
			soErr := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, gotSO)
			if tc.wantSOExists {
				require.NoError(t, soErr, "expected ScaledObject to exist")
				require.NotNil(t, gotSO.Spec.ScaleTargetRef, "SO scaleTargetRef should be set")
				assert.Equal(t, name, gotSO.Spec.ScaleTargetRef.Name, "SO target name should be the IR name")
				assert.Equal(t, "InferenceReplica", gotSO.Spec.ScaleTargetRef.Kind, "SO target kind should be InferenceReplica")
				require.Len(t, gotSO.OwnerReferences, 1, "SO should have exactly one ownerRef")
				assert.Equal(t, "InferenceReplica", gotSO.OwnerReferences[0].Kind)
			} else {
				assert.True(t, apierrors.IsNotFound(soErr), "expected SO to be absent, got err=%v", soErr)
			}
		})
	}
}

func TestDispatchAutoscaler_KEDARequiresTriggersBeforeMutation(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)

	tests := []struct {
		name       string
		autoscaler *v1beta1.ComponentAutoscaler
	}{
		{
			name:       "nil KEDA block",
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA},
		},
		{
			name: "empty KEDA triggers",
			autoscaler: &v1beta1.ComponentAutoscaler{
				Class: v1beta1.AutoscalerKEDA,
				Keda:  &v1beta1.KedaAutoscaler{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := dispatchScheme(t)
			hpa := existingHPA(ns, name, dispatchOwner(name))
			hpa.UID = types.UID("owned-hpa-uid")
			so := existingScaledObject(ns, name, dispatchOwner(name))
			so.UID = types.UID("owned-scaledobject-uid")
			originalHPASpec := hpa.DeepCopy().Spec
			originalSOSpec := so.DeepCopy().Spec
			mutations := []string{}
			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(hpa, so).
				WithInterceptorFuncs(interceptor.Funcs{
					Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
						mutations = append(mutations, "create "+obj.GetName())
						return c.Create(ctx, obj, opts...)
					},
					Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
						mutations = append(mutations, "update "+obj.GetName())
						return c.Update(ctx, obj, opts...)
					},
					Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
						mutations = append(mutations, "delete "+obj.GetName())
						return c.Delete(ctx, obj, opts...)
					},
				}).
				Build()

			err := DispatchAutoscaler(context.Background(), DispatchParams{
				Client:         cl,
				Scheme:         scheme,
				Owner:          dispatchOwner(name),
				Namespace:      ns,
				Name:           name,
				ScaleTargetRef: dispatchScaleTargetRef(name),
				Autoscaler:     tt.autoscaler,
				MinReplicas:    1,
				MaxReplicas:    5,
			})
			if assert.Error(t, err) {
				assert.Contains(t, err.Error(), "KEDA autoscaler requires at least one trigger")
			}
			assert.Empty(t, mutations, "invalid KEDA must fail before any scaler mutation")

			gotHPA := &autoscalingv2.HorizontalPodAutoscaler{}
			require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, gotHPA))
			assert.Equal(t, hpa.UID, gotHPA.UID)
			assert.Equal(t, originalHPASpec, gotHPA.Spec)

			gotSO := &kedav1.ScaledObject{}
			require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, gotSO))
			assert.Equal(t, so.UID, gotSO.UID)
			assert.Equal(t, originalSOSpec, gotSO.Spec)
		})
	}
}

func TestDispatchAutoscaler_KEDAReservedHPANameFailsBeforeMutation(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)

	tests := []struct {
		name        string
		existingHPA bool
	}{
		{name: "fresh dispatch"},
		{name: "HPA to KEDA", existingHPA: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := dispatchScheme(t)
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.existingHPA {
				builder = builder.WithObjects(existingHPA(ns, name, dispatchOwner(name)))
			}
			mutations := []string{}
			cl := builder.WithInterceptorFuncs(interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					mutations = append(mutations, "create "+obj.GetName())
					return c.Create(ctx, obj, opts...)
				},
				Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					mutations = append(mutations, "update "+obj.GetName())
					return c.Update(ctx, obj, opts...)
				},
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					mutations = append(mutations, "delete "+obj.GetName())
					return c.Delete(ctx, obj, opts...)
				},
			}).Build()

			kedaSpec := kedaAutoscalerBlock()
			kedaSpec.Advanced = &kedav1.AdvancedConfig{
				HorizontalPodAutoscalerConfig: &kedav1.HorizontalPodAutoscalerConfig{Name: name},
			}
			err := DispatchAutoscaler(context.Background(), DispatchParams{
				Client:         cl,
				Scheme:         scheme,
				Owner:          dispatchOwner(name),
				Namespace:      ns,
				Name:           name,
				ScaleTargetRef: dispatchScaleTargetRef(name),
				Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaSpec},
				MinReplicas:    1,
				MaxReplicas:    5,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "horizontalPodAutoscalerConfig.name")
			assert.Contains(t, err.Error(), name)
			assert.Empty(t, mutations)

			hpaErr := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &autoscalingv2.HorizontalPodAutoscaler{})
			if tt.existingHPA {
				require.NoError(t, hpaErr)
			} else {
				assert.True(t, apierrors.IsNotFound(hpaErr))
			}
			soErr := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, &kedav1.ScaledObject{})
			assert.True(t, apierrors.IsNotFound(soErr))
		})
	}
}

func TestDispatchAutoscaler_KEDACustomHPANameIsSupported(t *testing.T) {
	const (
		ns         = "test-ns"
		name       = "demo-engine"
		customName = "custom-keda-hpa"
	)
	scheme := dispatchScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	kedaSpec := kedaAutoscalerBlock()
	kedaSpec.Advanced = &kedav1.AdvancedConfig{
		HorizontalPodAutoscalerConfig: &kedav1.HorizontalPodAutoscalerConfig{Name: customName},
	}

	require.NoError(t, DispatchAutoscaler(context.Background(), DispatchParams{
		Client:         cl,
		Scheme:         scheme,
		Owner:          dispatchOwner(name),
		Namespace:      ns,
		Name:           name,
		ScaleTargetRef: dispatchScaleTargetRef(name),
		Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaSpec},
		MinReplicas:    1,
		MaxReplicas:    5,
	}))

	got := &kedav1.ScaledObject{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, got))
	require.NotNil(t, got.Spec.Advanced)
	require.NotNil(t, got.Spec.Advanced.HorizontalPodAutoscalerConfig)
	assert.Equal(t, customName, got.Spec.Advanced.HorizontalPodAutoscalerConfig.Name)
}

func TestDispatchAutoscaler_UnmanagedCleanupPreservesForeignScalers(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)

	classes := []v1beta1.AutoscalerClass{v1beta1.AutoscalerNone, v1beta1.AutoscalerExternal}
	resources := []string{"HPA", "ScaledObject"}
	owners := []struct {
		name string
		refs []metav1.OwnerReference
	}{
		{name: "ownerless"},
		{name: "different controller", refs: []metav1.OwnerReference{foreignDispatchOwner(name)}},
	}

	for _, class := range classes {
		for _, resource := range resources {
			for _, ownership := range owners {
				t.Run(string(class)+" preserves "+ownership.name+" "+resource, func(t *testing.T) {
					scheme := dispatchScheme(t)
					builder := fake.NewClientBuilder().WithScheme(scheme)
					if resource == "HPA" {
						builder = builder.WithObjects(existingHPA(ns, name, ownership.refs...))
					} else {
						builder = builder.WithObjects(existingScaledObject(ns, name, ownership.refs...))
					}
					cl := builder.Build()

					err := DispatchAutoscaler(context.Background(), DispatchParams{
						Client:         cl,
						Scheme:         scheme,
						Owner:          dispatchOwner(name),
						Namespace:      ns,
						Name:           name,
						ScaleTargetRef: dispatchScaleTargetRef(name),
						Autoscaler:     &v1beta1.ComponentAutoscaler{Class: class},
						MinReplicas:    1,
						MaxReplicas:    5,
					})
					require.NoError(t, err)

					if resource == "HPA" {
						got := &autoscalingv2.HorizontalPodAutoscaler{}
						require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, got))
						assert.Equal(t, ownership.refs, got.OwnerReferences)
					} else {
						got := &kedav1.ScaledObject{}
						require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, got))
						assert.Equal(t, ownership.refs, got.OwnerReferences)
					}
				})
			}
		}
	}
}

func TestDispatchAutoscaler_ModeOwnershipHandoff(t *testing.T) {
	const (
		ns       = "test-ns"
		isvcName = "demo"
		name     = "demo-engine"
	)
	labels := dispatchManagementLabels(isvcName, v1beta1.EngineComponent)

	directions := []struct {
		name       string
		fromOwner  metav1.OwnerReference
		toOwner    metav1.OwnerReference
		fromTarget autoscalingv2.CrossVersionObjectReference
		toTarget   autoscalingv2.CrossVersionObjectReference
	}{
		{
			name:       "Raw to OMENative",
			fromOwner:  dispatchISVCOwner(isvcName),
			toOwner:    dispatchOwner(name),
			fromTarget: dispatchDeploymentTargetRef(name),
			toTarget:   dispatchScaleTargetRef(name),
		},
		{
			name:       "OMENative to Raw",
			fromOwner:  dispatchOwner(name),
			toOwner:    dispatchISVCOwner(isvcName),
			fromTarget: dispatchScaleTargetRef(name),
			toTarget:   dispatchDeploymentTargetRef(name),
		},
	}

	classes := []struct {
		name       string
		autoscaler *v1beta1.ComponentAutoscaler
		resource   string
	}{
		{name: "HPA", autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}, resource: "HPA"},
		{name: "KEDA", autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()}, resource: "ScaledObject"},
	}

	for _, direction := range directions {
		for _, class := range classes {
			t.Run(direction.name+" "+class.name, func(t *testing.T) {
				scheme := dispatchScheme(t)
				var existing client.Object
				if class.resource == "HPA" {
					hpa := existingHPA(ns, name, direction.fromOwner)
					hpa.Labels = cloneStringMap(labels)
					hpa.Spec.ScaleTargetRef = direction.fromTarget
					existing = hpa
				} else {
					so := existingScaledObject(ns, name, direction.fromOwner)
					so.Labels = cloneStringMap(labels)
					so.Spec.ScaleTargetRef = &kedav1.ScaleTarget{
						APIVersion: direction.fromTarget.APIVersion,
						Kind:       direction.fromTarget.Kind,
						Name:       direction.fromTarget.Name,
					}
					existing = so
				}
				cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dispatchModeBridgeIR(ns, isvcName, name), existing).Build()

				err := DispatchAutoscaler(context.Background(), DispatchParams{
					Client:         cl,
					Scheme:         scheme,
					Owner:          direction.toOwner,
					Namespace:      ns,
					Name:           name,
					Labels:         cloneStringMap(labels),
					ScaleTargetRef: direction.toTarget,
					Autoscaler:     class.autoscaler,
					MinReplicas:    1,
					MaxReplicas:    5,
				})
				require.NoError(t, err)

				if class.resource == "HPA" {
					got := &autoscalingv2.HorizontalPodAutoscaler{}
					require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, got))
					assert.Equal(t, direction.toTarget, got.Spec.ScaleTargetRef)
					assert.Equal(t, direction.toOwner, *metav1.GetControllerOf(got))
					assert.Equal(t, labels, got.Labels)
				} else {
					got := &kedav1.ScaledObject{}
					require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, got))
					require.NotNil(t, got.Spec.ScaleTargetRef)
					assert.Equal(t, direction.toTarget.APIVersion, got.Spec.ScaleTargetRef.APIVersion)
					assert.Equal(t, direction.toTarget.Kind, got.Spec.ScaleTargetRef.Kind)
					assert.Equal(t, direction.toTarget.Name, got.Spec.ScaleTargetRef.Name)
					assert.Equal(t, direction.toOwner, *metav1.GetControllerOf(got))
					assert.Equal(t, labels, got.Labels)
				}
			})
		}
	}
}

func TestDispatchAutoscaler_ModeHandoffRecognizesUnlabeledIRScaler(t *testing.T) {
	const (
		ns       = "test-ns"
		isvcName = "demo"
		name     = "demo-engine"
	)
	labels := dispatchManagementLabels(isvcName, v1beta1.EngineComponent)
	isvcOwner := dispatchISVCOwner(isvcName)
	irOwner := dispatchOwner(name)
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       ns,
			UID:             irOwner.UID,
			Labels:          cloneStringMap(labels),
			OwnerReferences: []metav1.OwnerReference{isvcOwner},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: &v1beta1.ParentReference{Name: isvcName},
			Component: v1beta1.EngineComponent,
		},
	}

	tests := []struct {
		name       string
		autoscaler *v1beta1.ComponentAutoscaler
		resource   string
		wantExists bool
	}{
		{name: "HPA takeover", autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}, resource: "HPA", wantExists: true},
		{name: "KEDA takeover", autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()}, resource: "ScaledObject", wantExists: true},
		{name: "None cleanup", autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}, resource: "HPA"},
		{name: "External cleanup", autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}, resource: "ScaledObject"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := dispatchScheme(t)
			var scaler client.Object
			if tt.resource == "HPA" {
				hpa := existingHPA(ns, name, irOwner)
				hpa.Labels = nil
				scaler = hpa
			} else {
				so := existingScaledObject(ns, name, irOwner)
				so.Labels = nil
				scaler = so
			}
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ir.DeepCopy(), scaler).Build()

			err := DispatchAutoscaler(context.Background(), DispatchParams{
				Client:         cl,
				Scheme:         scheme,
				Owner:          isvcOwner,
				Namespace:      ns,
				Name:           name,
				Labels:         cloneStringMap(labels),
				ScaleTargetRef: dispatchDeploymentTargetRef(name),
				Autoscaler:     tt.autoscaler,
				MinReplicas:    1,
				MaxReplicas:    5,
			})
			require.NoError(t, err)

			if tt.resource == "HPA" {
				got := &autoscalingv2.HorizontalPodAutoscaler{}
				getErr := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, got)
				if tt.wantExists {
					require.NoError(t, getErr)
					assert.Equal(t, isvcOwner, *metav1.GetControllerOf(got))
					assert.Equal(t, labels, got.Labels)
				} else {
					assert.True(t, apierrors.IsNotFound(getErr))
				}
			} else {
				got := &kedav1.ScaledObject{}
				getErr := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, got)
				if tt.wantExists {
					require.NoError(t, getErr)
					assert.Equal(t, isvcOwner, *metav1.GetControllerOf(got))
					assert.Equal(t, labels, got.Labels)
				} else {
					assert.True(t, apierrors.IsNotFound(getErr))
				}
			}
		})
	}

	t.Run("explicitly mismatched scaler labels remain protected", func(t *testing.T) {
		scheme := dispatchScheme(t)
		hpa := existingHPA(ns, name, irOwner)
		hpa.Labels = dispatchManagementLabels(isvcName, v1beta1.DecoderComponent)
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ir.DeepCopy(), hpa).Build()

		err := DispatchAutoscaler(context.Background(), DispatchParams{
			Client:         cl,
			Scheme:         scheme,
			Owner:          isvcOwner,
			Namespace:      ns,
			Name:           name,
			Labels:         cloneStringMap(labels),
			ScaleTargetRef: dispatchDeploymentTargetRef(name),
			Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			MinReplicas:    1,
			MaxReplicas:    5,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not controlled by expected owner")
		require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &autoscalingv2.HorizontalPodAutoscaler{}))
	})
}

func TestDispatchAutoscaler_ModeHandoffRequiresVerifiedIRBridge(t *testing.T) {
	const (
		ns       = "test-ns"
		isvcName = "demo"
		name     = "demo-engine"
	)
	desiredLabels := dispatchManagementLabels(isvcName, v1beta1.EngineComponent)
	newISVCOwner := dispatchISVCOwner(isvcName)
	newISVCOwner.UID = "new-isvc-uid"
	oldISVCOwner := newISVCOwner
	oldISVCOwner.UID = "old-isvc-uid"
	irOwner := dispatchOwner(name)
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       ns,
			UID:             irOwner.UID,
			Labels:          cloneStringMap(desiredLabels),
			OwnerReferences: []metav1.OwnerReference{newISVCOwner},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: &v1beta1.ParentReference{Name: isvcName},
			Component: v1beta1.EngineComponent,
		},
	}

	tests := []struct {
		name       string
		resource   string
		owner      metav1.OwnerReference
		labels     map[string]string
		autoscaler *v1beta1.ComponentAutoscaler
	}{
		{
			name:       "HPA owner does not match live IR parent",
			resource:   "HPA",
			owner:      oldISVCOwner,
			labels:     desiredLabels,
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
		},
		{
			name:       "ScaledObject owner does not match live IR parent",
			resource:   "ScaledObject",
			owner:      oldISVCOwner,
			labels:     desiredLabels,
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
		},
		{
			name:       "mismatched component labels remain protected",
			resource:   "HPA",
			owner:      newISVCOwner,
			labels:     dispatchManagementLabels(isvcName, v1beta1.DecoderComponent),
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := dispatchScheme(t)
			var scaler client.Object
			if tt.resource == "HPA" {
				hpa := existingHPA(ns, name, tt.owner)
				hpa.Labels = cloneStringMap(tt.labels)
				hpa.Finalizers = []string{"example.com/finalizer"}
				scaler = hpa
			} else {
				so := existingScaledObject(ns, name, tt.owner)
				so.Labels = cloneStringMap(tt.labels)
				so.Finalizers = []string{"example.com/finalizer"}
				scaler = so
			}
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ir.DeepCopy(), scaler).Build()

			err := DispatchAutoscaler(context.Background(), DispatchParams{
				Client:         cl,
				Scheme:         scheme,
				Owner:          irOwner,
				Namespace:      ns,
				Name:           name,
				Labels:         cloneStringMap(desiredLabels),
				ScaleTargetRef: dispatchScaleTargetRef(name),
				Autoscaler:     tt.autoscaler,
				MinReplicas:    1,
				MaxReplicas:    5,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not controlled by expected owner")

			if tt.resource == "HPA" {
				got := &autoscalingv2.HorizontalPodAutoscaler{}
				require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, got))
				assert.Equal(t, tt.owner, *metav1.GetControllerOf(got))
				assert.Equal(t, []string{"example.com/finalizer"}, got.Finalizers)
			} else {
				got := &kedav1.ScaledObject{}
				require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, got))
				assert.Equal(t, tt.owner, *metav1.GetControllerOf(got))
				assert.Equal(t, []string{"example.com/finalizer"}, got.Finalizers)
			}
		})
	}
}

func TestDispatchAutoscaler_UnmanagedCleanupDeletesVerifiedModeBridgeScalers(t *testing.T) {
	const (
		ns       = "test-ns"
		isvcName = "demo"
		name     = "demo-engine"
	)
	labels := dispatchManagementLabels(isvcName, v1beta1.EngineComponent)
	directions := []struct {
		name      string
		fromOwner metav1.OwnerReference
		toOwner   metav1.OwnerReference
	}{
		{name: "Raw to OMENative", fromOwner: dispatchISVCOwner(isvcName), toOwner: dispatchOwner(name)},
		{name: "OMENative to Raw", fromOwner: dispatchOwner(name), toOwner: dispatchISVCOwner(isvcName)},
	}

	for _, direction := range directions {
		for _, class := range []v1beta1.AutoscalerClass{v1beta1.AutoscalerNone, v1beta1.AutoscalerExternal} {
			for _, resource := range []string{"HPA", "ScaledObject"} {
				t.Run(direction.name+" "+string(class)+" deletes "+resource, func(t *testing.T) {
					scheme := dispatchScheme(t)
					var existing client.Object
					if resource == "HPA" {
						hpa := existingHPA(ns, name, direction.fromOwner)
						hpa.Labels = cloneStringMap(labels)
						existing = hpa
					} else {
						so := existingScaledObject(ns, name, direction.fromOwner)
						so.Labels = cloneStringMap(labels)
						existing = so
					}
					cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dispatchModeBridgeIR(ns, isvcName, name), existing).Build()

					require.NoError(t, DispatchAutoscaler(context.Background(), DispatchParams{
						Client:         cl,
						Scheme:         scheme,
						Owner:          direction.toOwner,
						Namespace:      ns,
						Name:           name,
						Labels:         cloneStringMap(labels),
						ScaleTargetRef: dispatchScaleTargetRef(name),
						Autoscaler:     &v1beta1.ComponentAutoscaler{Class: class},
						MinReplicas:    1,
						MaxReplicas:    5,
					}))

					if resource == "HPA" {
						err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &autoscalingv2.HorizontalPodAutoscaler{})
						assert.True(t, apierrors.IsNotFound(err), "verified mode-bridge HPA must be deleted")
					} else {
						err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, &kedav1.ScaledObject{})
						assert.True(t, apierrors.IsNotFound(err), "verified mode-bridge ScaledObject must be deleted")
					}
				})
			}
		}
	}
}

func TestDispatchAutoscaler_ModeBridgeRequiresMatchingLabels(t *testing.T) {
	const (
		ns       = "test-ns"
		isvcName = "demo"
		name     = "demo-engine"
	)
	wantLabels := dispatchManagementLabels(isvcName, v1beta1.EngineComponent)
	mismatchedLabels := dispatchManagementLabels(isvcName, v1beta1.DecoderComponent)

	for _, resource := range []string{"HPA", "ScaledObject"} {
		t.Run("managed class rejects mismatched "+resource, func(t *testing.T) {
			scheme := dispatchScheme(t)
			var existing client.Object
			var requested *v1beta1.ComponentAutoscaler
			if resource == "HPA" {
				hpa := existingHPA(ns, name, dispatchISVCOwner(isvcName))
				hpa.Labels = cloneStringMap(mismatchedLabels)
				existing = hpa
				requested = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}
			} else {
				so := existingScaledObject(ns, name, dispatchISVCOwner(isvcName))
				so.Labels = cloneStringMap(mismatchedLabels)
				existing = so
				requested = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()}
			}
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dispatchModeBridgeIR(ns, isvcName, name), existing).Build()

			err := DispatchAutoscaler(context.Background(), DispatchParams{
				Client:         cl,
				Scheme:         scheme,
				Owner:          dispatchOwner(name),
				Namespace:      ns,
				Name:           name,
				Labels:         cloneStringMap(wantLabels),
				ScaleTargetRef: dispatchScaleTargetRef(name),
				Autoscaler:     requested,
				MinReplicas:    1,
				MaxReplicas:    5,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not controlled by expected owner")
		})

		t.Run("unmanaged class preserves mismatched "+resource, func(t *testing.T) {
			scheme := dispatchScheme(t)
			var existing client.Object
			if resource == "HPA" {
				hpa := existingHPA(ns, name, dispatchISVCOwner(isvcName))
				hpa.Labels = cloneStringMap(mismatchedLabels)
				existing = hpa
			} else {
				so := existingScaledObject(ns, name, dispatchISVCOwner(isvcName))
				so.Labels = cloneStringMap(mismatchedLabels)
				existing = so
			}
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dispatchModeBridgeIR(ns, isvcName, name), existing).Build()

			require.NoError(t, DispatchAutoscaler(context.Background(), DispatchParams{
				Client:         cl,
				Scheme:         scheme,
				Owner:          dispatchOwner(name),
				Namespace:      ns,
				Name:           name,
				Labels:         cloneStringMap(wantLabels),
				ScaleTargetRef: dispatchScaleTargetRef(name),
				Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
				MinReplicas:    1,
				MaxReplicas:    5,
			}))

			if resource == "HPA" {
				require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &autoscalingv2.HorizontalPodAutoscaler{}))
			} else {
				require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, &kedav1.ScaledObject{}))
			}
		})
	}
}

func TestDispatchAutoscaler_ManagedClassRejectsForeignScalers(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)

	tests := []struct {
		name       string
		requested  *v1beta1.ComponentAutoscaler
		existing   string
		ownerRefs  []metav1.OwnerReference
		wantNoHPA  bool
		wantNoKEDA bool
	}{
		{
			name:      "HPA rejects ownerless HPA",
			requested: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			existing:  "HPA",
		},
		{
			name:      "HPA rejects differently controlled HPA",
			requested: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			existing:  "HPA",
			ownerRefs: []metav1.OwnerReference{foreignDispatchOwner(name)},
		},
		{
			name:      "HPA rejects matching non-controller owner reference",
			requested: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			existing:  "HPA",
			ownerRefs: []metav1.OwnerReference{nonControllerDispatchOwner(name)},
		},
		{
			name:       "HPA rejects ownerless ScaledObject transition",
			requested:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			existing:   "ScaledObject",
			wantNoHPA:  true,
			wantNoKEDA: false,
		},
		{
			name:       "HPA rejects differently controlled ScaledObject transition",
			requested:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			existing:   "ScaledObject",
			ownerRefs:  []metav1.OwnerReference{foreignDispatchOwner(name)},
			wantNoHPA:  true,
			wantNoKEDA: false,
		},
		{
			name:      "KEDA rejects ownerless ScaledObject",
			requested: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
			existing:  "ScaledObject",
		},
		{
			name:      "KEDA rejects differently controlled ScaledObject",
			requested: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
			existing:  "ScaledObject",
			ownerRefs: []metav1.OwnerReference{foreignDispatchOwner(name)},
		},
		{
			name:      "KEDA rejects matching non-controller owner reference",
			requested: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
			existing:  "ScaledObject",
			ownerRefs: []metav1.OwnerReference{nonControllerDispatchOwner(name)},
		},
		{
			name:       "KEDA rejects ownerless HPA transition",
			requested:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
			existing:   "HPA",
			wantNoHPA:  false,
			wantNoKEDA: true,
		},
		{
			name:       "KEDA rejects differently controlled HPA transition",
			requested:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
			existing:   "HPA",
			ownerRefs:  []metav1.OwnerReference{foreignDispatchOwner(name)},
			wantNoHPA:  false,
			wantNoKEDA: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := dispatchScheme(t)
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.existing == "HPA" {
				builder = builder.WithObjects(existingHPA(ns, name, tt.ownerRefs...))
			} else {
				builder = builder.WithObjects(existingScaledObject(ns, name, tt.ownerRefs...))
			}
			cl := builder.Build()

			err := DispatchAutoscaler(context.Background(), DispatchParams{
				Client:         cl,
				Scheme:         scheme,
				Owner:          dispatchOwner(name),
				Namespace:      ns,
				Name:           name,
				ScaleTargetRef: dispatchScaleTargetRef(name),
				Autoscaler:     tt.requested,
				MinReplicas:    1,
				MaxReplicas:    5,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not controlled by expected owner")

			if tt.existing == "HPA" {
				got := &autoscalingv2.HorizontalPodAutoscaler{}
				require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, got))
				assert.Equal(t, int32(3), got.Spec.MaxReplicas)
				assert.Equal(t, tt.ownerRefs, got.OwnerReferences)
			} else {
				got := &kedav1.ScaledObject{}
				require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, got))
				require.NotNil(t, got.Spec.MaxReplicaCount)
				assert.Equal(t, int32(3), *got.Spec.MaxReplicaCount)
				assert.Equal(t, tt.ownerRefs, got.OwnerReferences)
			}

			if tt.wantNoHPA {
				err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &autoscalingv2.HorizontalPodAutoscaler{})
				assert.True(t, apierrors.IsNotFound(err), "requested HPA must not be created when the ScaledObject is foreign")
			}
			if tt.wantNoKEDA {
				err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, &kedav1.ScaledObject{})
				assert.True(t, apierrors.IsNotFound(err), "requested ScaledObject must not be created when the HPA is foreign")
			}
		})
	}
}

func TestDispatchAutoscaler_OwnershipConflictDoesNotPartiallyMutate(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)

	tests := []struct {
		name       string
		autoscaler *v1beta1.ComponentAutoscaler
		hpaOwner   metav1.OwnerReference
		soOwner    metav1.OwnerReference
	}{
		{
			name:       "HPA dispatch preserves owned HPA when ScaledObject is foreign",
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			hpaOwner:   dispatchOwner(name),
			soOwner:    foreignDispatchOwner(name),
		},
		{
			name:       "HPA dispatch preserves owned ScaledObject when HPA is foreign",
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			hpaOwner:   foreignDispatchOwner(name),
			soOwner:    dispatchOwner(name),
		},
		{
			name:       "KEDA dispatch preserves owned HPA when ScaledObject is foreign",
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
			hpaOwner:   dispatchOwner(name),
			soOwner:    foreignDispatchOwner(name),
		},
		{
			name:       "KEDA dispatch preserves owned ScaledObject when HPA is foreign",
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
			hpaOwner:   foreignDispatchOwner(name),
			soOwner:    dispatchOwner(name),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := dispatchScheme(t)
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
				existingHPA(ns, name, tt.hpaOwner),
				existingScaledObject(ns, name, tt.soOwner),
			).Build()

			err := DispatchAutoscaler(context.Background(), DispatchParams{
				Client:         cl,
				Scheme:         scheme,
				Owner:          dispatchOwner(name),
				Namespace:      ns,
				Name:           name,
				ScaleTargetRef: dispatchScaleTargetRef(name),
				Autoscaler:     tt.autoscaler,
				MinReplicas:    1,
				MaxReplicas:    5,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not controlled by expected owner")

			gotHPA := &autoscalingv2.HorizontalPodAutoscaler{}
			require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, gotHPA))
			assert.Equal(t, []metav1.OwnerReference{tt.hpaOwner}, gotHPA.OwnerReferences)
			assert.Equal(t, int32(3), gotHPA.Spec.MaxReplicas)

			gotSO := &kedav1.ScaledObject{}
			require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, gotSO))
			assert.Equal(t, []metav1.OwnerReference{tt.soOwner}, gotSO.OwnerReferences)
			require.NotNil(t, gotSO.Spec.MaxReplicaCount)
			assert.Equal(t, int32(3), *gotSO.Spec.MaxReplicaCount)
		})
	}
}

func TestDispatchAutoscaler_ManagedTransitionRejectsForeignReplacement(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)

	t.Run("HPA rejects ScaledObject replaced after preflight", func(t *testing.T) {
		scheme := dispatchScheme(t)
		foreign := existingScaledObject(ns, name, foreignDispatchOwner(name))
		foreign.UID = types.UID("foreign-scaledobject-uid")
		owned := existingScaledObject(ns, name, dispatchOwner(name))
		owned.UID = types.UID("owned-scaledobject-uid")
		gets := 0
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(foreign).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if scaledObject, ok := obj.(*kedav1.ScaledObject); ok && key.Name == utils.GetScaledObjectName(name) {
					gets++
					if gets == 1 {
						*scaledObject = *owned.DeepCopy()
						return nil
					}
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).Build()

		err := DispatchAutoscaler(context.Background(), DispatchParams{
			Client:         cl,
			Scheme:         scheme,
			Owner:          dispatchOwner(name),
			Namespace:      ns,
			Name:           name,
			ScaleTargetRef: dispatchScaleTargetRef(name),
			Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			MinReplicas:    1,
			MaxReplicas:    5,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not controlled by expected owner")

		gotSO := &kedav1.ScaledObject{}
		require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, gotSO))
		assert.Equal(t, foreign.UID, gotSO.UID)
		gotHPA := &autoscalingv2.HorizontalPodAutoscaler{}
		require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, gotHPA))
		assert.Equal(t, dispatchOwner(name), *metav1.GetControllerOf(gotHPA), "requested HPA should remain while the foreign ScaledObject blocks cleanup")
	})

	t.Run("KEDA rejects HPA replaced after preflight", func(t *testing.T) {
		scheme := dispatchScheme(t)
		foreign := existingHPA(ns, name, foreignDispatchOwner(name))
		foreign.UID = types.UID("foreign-hpa-uid")
		owned := existingHPA(ns, name, dispatchOwner(name))
		owned.UID = types.UID("owned-hpa-uid")
		gets := 0
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(foreign).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if hpa, ok := obj.(*autoscalingv2.HorizontalPodAutoscaler); ok && key.Name == name {
					gets++
					if gets == 1 {
						*hpa = *owned.DeepCopy()
						return nil
					}
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).Build()

		err := DispatchAutoscaler(context.Background(), DispatchParams{
			Client:         cl,
			Scheme:         scheme,
			Owner:          dispatchOwner(name),
			Namespace:      ns,
			Name:           name,
			ScaleTargetRef: dispatchScaleTargetRef(name),
			Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
			MinReplicas:    1,
			MaxReplicas:    5,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not controlled by expected owner")

		gotHPA := &autoscalingv2.HorizontalPodAutoscaler{}
		require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, gotHPA))
		assert.Equal(t, foreign.UID, gotHPA.UID)
		gotSO := &kedav1.ScaledObject{}
		require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, gotSO))
		assert.Equal(t, dispatchOwner(name), *metav1.GetControllerOf(gotSO), "requested ScaledObject should remain while the foreign HPA blocks cleanup")
	})
}

func TestDispatchAutoscaler_ClassSwitchPreservesWorkingScalerWhenReplacementFails(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)

	t.Run("HPA to KEDA preserves HPA when ScaledObject create fails", func(t *testing.T) {
		scheme := dispatchScheme(t)
		hpa := existingHPA(ns, name, dispatchOwner(name))
		hpa.UID = types.UID("working-hpa-uid")
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(hpa).WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*kedav1.ScaledObject); ok {
					return &meta.NoKindMatchError{
						GroupKind:        schema.GroupKind{Group: kedav1.GroupVersion.Group, Kind: "ScaledObject"},
						SearchedVersions: []string{kedav1.GroupVersion.Version},
					}
				}
				return c.Create(ctx, obj, opts...)
			},
		}).Build()

		err := DispatchAutoscaler(context.Background(), DispatchParams{
			Client:         cl,
			Scheme:         scheme,
			Owner:          dispatchOwner(name),
			Namespace:      ns,
			Name:           name,
			ScaleTargetRef: dispatchScaleTargetRef(name),
			Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
			MinReplicas:    1,
			MaxReplicas:    5,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ScaledObject reconcile")

		got := &autoscalingv2.HorizontalPodAutoscaler{}
		require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, got))
		assert.Equal(t, hpa.UID, got.UID)
		soErr := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, &kedav1.ScaledObject{})
		assert.True(t, apierrors.IsNotFound(soErr))
	})

	t.Run("KEDA to HPA preserves ScaledObject when HPA create fails", func(t *testing.T) {
		scheme := dispatchScheme(t)
		so := existingScaledObject(ns, name, dispatchOwner(name))
		so.UID = types.UID("working-scaledobject-uid")
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(so).WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*autoscalingv2.HorizontalPodAutoscaler); ok {
					return errors.New("injected HPA create failure")
				}
				return c.Create(ctx, obj, opts...)
			},
		}).Build()

		err := DispatchAutoscaler(context.Background(), DispatchParams{
			Client:         cl,
			Scheme:         scheme,
			Owner:          dispatchOwner(name),
			Namespace:      ns,
			Name:           name,
			ScaleTargetRef: dispatchScaleTargetRef(name),
			Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			MinReplicas:    1,
			MaxReplicas:    5,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "HPA reconcile")

		got := &kedav1.ScaledObject{}
		require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, got))
		assert.Equal(t, so.UID, got.UID)
		hpaErr := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &autoscalingv2.HorizontalPodAutoscaler{})
		assert.True(t, apierrors.IsNotFound(hpaErr))
	})
}

func TestDispatchAutoscaler_CurrentOwnerMetadataConverges(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)

	tests := []struct {
		name       string
		autoscaler *v1beta1.ComponentAutoscaler
		resource   string
	}{
		{
			name:       "HPA",
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			resource:   "HPA",
		},
		{
			name:       "ScaledObject",
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
			resource:   "ScaledObject",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := dispatchScheme(t)
			cl := fake.NewClientBuilder().WithScheme(scheme).Build()
			desiredOwner := dispatchOwner(name)
			desiredOwner.BlockOwnerDeletion = ptr.To(true)
			params := DispatchParams{
				Client:         cl,
				Scheme:         scheme,
				Owner:          desiredOwner,
				Namespace:      ns,
				Name:           name,
				ScaleTargetRef: dispatchScaleTargetRef(name),
				Autoscaler:     tt.autoscaler,
				MinReplicas:    1,
				MaxReplicas:    5,
			}
			require.NoError(t, DispatchAutoscaler(context.Background(), params))

			staleOwner := desiredOwner
			staleOwner.APIVersion = "stale.example/v1"
			staleOwner.Kind = "StaleOwner"
			staleOwner.Name = "stale-owner"
			staleOwner.BlockOwnerDeletion = nil
			if tt.resource == "HPA" {
				obj := &autoscalingv2.HorizontalPodAutoscaler{}
				require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, obj))
				obj.OwnerReferences = []metav1.OwnerReference{staleOwner}
				require.NoError(t, cl.Update(context.Background(), obj))
			} else {
				obj := &kedav1.ScaledObject{}
				require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, obj))
				obj.OwnerReferences = []metav1.OwnerReference{staleOwner}
				require.NoError(t, cl.Update(context.Background(), obj))
			}

			require.NoError(t, DispatchAutoscaler(context.Background(), params))

			if tt.resource == "HPA" {
				got := &autoscalingv2.HorizontalPodAutoscaler{}
				require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, got))
				assert.Equal(t, []metav1.OwnerReference{desiredOwner}, got.OwnerReferences)
			} else {
				got := &kedav1.ScaledObject{}
				require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, got))
				assert.Equal(t, []metav1.OwnerReference{desiredOwner}, got.OwnerReferences)
			}
		})
	}
}

func TestDispatchAutoscaler_PropagatedMetadataRemovalPreservesInjectedMetadata(t *testing.T) {
	const (
		ns               = "test-ns"
		name             = "demo-engine"
		pausedAnnotation = "autoscaling.keda.sh/paused"
	)
	stableLabels := dispatchManagementLabels("demo", v1beta1.EngineComponent)

	tests := []struct {
		name       string
		autoscaler *v1beta1.ComponentAutoscaler
		resource   string
	}{
		{name: "HPA", autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}, resource: "HPA"},
		{name: "ScaledObject", autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()}, resource: "ScaledObject"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := dispatchScheme(t)
			cl := fake.NewClientBuilder().WithScheme(scheme).Build()
			initialLabels := cloneStringMap(stableLabels)
			initialLabels["example.com/removed-label"] = "old"
			params := DispatchParams{
				Client:         cl,
				Scheme:         scheme,
				Owner:          dispatchOwner(name),
				Namespace:      ns,
				Name:           name,
				Labels:         initialLabels,
				Annotations:    map[string]string{pausedAnnotation: "true", "example.com/removed-note": "old"},
				ScaleTargetRef: dispatchScaleTargetRef(name),
				Autoscaler:     tt.autoscaler,
				MinReplicas:    1,
				MaxReplicas:    5,
			}
			require.NoError(t, DispatchAutoscaler(context.Background(), params))

			var live client.Object
			if tt.resource == "HPA" {
				live = &autoscalingv2.HorizontalPodAutoscaler{}
				require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, live))
			} else {
				live = &kedav1.ScaledObject{}
				require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, live))
			}
			labels := live.GetLabels()
			labels["example.com/injected-label"] = "keep"
			live.SetLabels(labels)
			annotations := live.GetAnnotations()
			annotations["example.com/injected-note"] = "keep"
			live.SetAnnotations(annotations)
			live.SetFinalizers([]string{"example.com/finalizer"})
			require.NoError(t, cl.Update(context.Background(), live))

			params.Labels = cloneStringMap(stableLabels)
			params.Annotations = nil
			require.NoError(t, DispatchAutoscaler(context.Background(), params))

			if tt.resource == "HPA" {
				live = &autoscalingv2.HorizontalPodAutoscaler{}
				require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, live))
			} else {
				live = &kedav1.ScaledObject{}
				require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, live))
			}
			assert.NotContains(t, live.GetLabels(), "example.com/removed-label")
			assert.NotContains(t, live.GetAnnotations(), pausedAnnotation)
			assert.NotContains(t, live.GetAnnotations(), "example.com/removed-note")
			assert.Equal(t, "keep", live.GetLabels()["example.com/injected-label"])
			assert.Equal(t, "keep", live.GetAnnotations()["example.com/injected-note"])
			assert.Equal(t, []string{"example.com/finalizer"}, live.GetFinalizers())
		})
	}
}

func TestDispatchAutoscaler_AdoptsUntrackedMetadataConservatively(t *testing.T) {
	const (
		ns                 = "test-ns"
		name               = "demo-engine"
		pausedAnnotation   = "autoscaling.keda.sh/paused"
		managedLabel       = "example.com/managed-label"
		managedAnnotation  = "example.com/managed-note"
		injectedLabel      = "example.com/injected-label"
		injectedAnnotation = "example.com/injected-note"
	)
	stableLabels := dispatchManagementLabels("demo", v1beta1.EngineComponent)
	untracked := existingScaledObject(ns, name, dispatchOwner(name))
	untracked.Labels = cloneStringMap(stableLabels)
	untracked.Labels[injectedLabel] = "keep"
	untracked.Annotations = map[string]string{
		pausedAnnotation:                      "true",
		constants.TargetUtilizationPercentage: "71",
		injectedAnnotation:                    "keep",
	}

	scheme := dispatchScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(untracked).Build()
	desiredLabels := cloneStringMap(stableLabels)
	desiredLabels[managedLabel] = "current"
	params := DispatchParams{
		Client:         cl,
		Scheme:         scheme,
		Owner:          dispatchOwner(name),
		Namespace:      ns,
		Name:           name,
		Labels:         desiredLabels,
		Annotations:    map[string]string{managedAnnotation: "current"},
		ScaleTargetRef: dispatchScaleTargetRef(name),
		Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
		MinReplicas:    1,
		MaxReplicas:    5,
	}

	require.NoError(t, DispatchAutoscaler(context.Background(), params))
	key := types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}
	adopted := &kedav1.ScaledObject{}
	require.NoError(t, cl.Get(context.Background(), key, adopted))
	assert.Equal(t, "keep", adopted.Labels[injectedLabel])
	assert.Equal(t, "keep", adopted.Annotations[injectedAnnotation])
	assert.Equal(t, "current", adopted.Labels[managedLabel])
	assert.Equal(t, "current", adopted.Annotations[managedAnnotation])
	assert.Equal(t, "true", adopted.Annotations[pausedAnnotation])
	assert.Equal(t, "71", adopted.Annotations[constants.TargetUtilizationPercentage])
	assert.NotEmpty(t, adopted.Annotations[constants.AutoscalerPropagatedMetadataKeys])

	params.Labels = cloneStringMap(stableLabels)
	params.Annotations = nil
	require.NoError(t, DispatchAutoscaler(context.Background(), params))
	converged := &kedav1.ScaledObject{}
	require.NoError(t, cl.Get(context.Background(), key, converged))
	assert.NotContains(t, converged.Labels, managedLabel)
	assert.NotContains(t, converged.Annotations, managedAnnotation)
	assert.Equal(t, "keep", converged.Labels[injectedLabel])
	assert.Equal(t, "keep", converged.Annotations[injectedAnnotation])
	assert.Equal(t, "true", converged.Annotations[pausedAnnotation])
	assert.Equal(t, "71", converged.Annotations[constants.TargetUtilizationPercentage])
}

func TestDeleteManagedAutoscalersUsesUIDPrecondition(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)

	tests := []struct {
		name      string
		object    client.Object
		deleteObj func(client.Client) error
	}{
		{
			name:   "HPA",
			object: existingHPA(ns, name, dispatchOwner(name)),
			deleteObj: func(c client.Client) error {
				return deleteHPAIfExists(context.Background(), c, ns, name, dispatchOwner(name), nil, rejectForeignScaler)
			},
		},
		{
			name:   "ScaledObject",
			object: existingScaledObject(ns, name, dispatchOwner(name)),
			deleteObj: func(c client.Client) error {
				return deleteScaledObjectIfExists(context.Background(), c, ns, name, dispatchOwner(name), nil, rejectForeignScaler)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.object.SetUID(types.UID("managed-scaler-uid"))
			tt.object.SetResourceVersion("managed-scaler-resource-version")
			scheme := dispatchScheme(t)
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.object).WithInterceptorFuncs(interceptor.Funcs{
				Delete: func(_ context.Context, _ client.WithWatch, _ client.Object, opts ...client.DeleteOption) error {
					deleteOptions := &client.DeleteOptions{}
					for _, opt := range opts {
						opt.ApplyToDelete(deleteOptions)
					}
					require.NotNil(t, deleteOptions.Preconditions)
					require.NotNil(t, deleteOptions.Preconditions.UID)
					assert.Equal(t, tt.object.GetUID(), *deleteOptions.Preconditions.UID)
					require.NotNil(t, deleteOptions.Preconditions.ResourceVersion)
					assert.Equal(t, tt.object.GetResourceVersion(), *deleteOptions.Preconditions.ResourceVersion)
					return nil
				},
			}).Build()

			require.NoError(t, tt.deleteObj(cl))
		})
	}
}

func TestDeleteManagedAutoscalersRejectsSameUIDOwnerHandoffRace(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)

	tests := []struct {
		name      string
		object    client.Object
		deleteObj func(client.Client) error
		key       types.NamespacedName
		fresh     func() client.Object
	}{
		{
			name:   "HPA",
			object: existingHPA(ns, name, dispatchOwner(name)),
			deleteObj: func(c client.Client) error {
				return deleteHPAIfExists(context.Background(), c, ns, name, dispatchOwner(name), nil, rejectForeignScaler)
			},
			key:   types.NamespacedName{Namespace: ns, Name: name},
			fresh: func() client.Object { return &autoscalingv2.HorizontalPodAutoscaler{} },
		},
		{
			name:   "ScaledObject",
			object: existingScaledObject(ns, name, dispatchOwner(name)),
			deleteObj: func(c client.Client) error {
				return deleteScaledObjectIfExists(context.Background(), c, ns, name, dispatchOwner(name), nil, rejectForeignScaler)
			},
			key:   types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)},
			fresh: func() client.Object { return &kedav1.ScaledObject{} },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.object.SetUID(types.UID("stable-scaler-uid"))
			tt.object.SetResourceVersion("1")
			scheme := dispatchScheme(t)
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.object).WithInterceptorFuncs(interceptor.Funcs{
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					replacement := obj.DeepCopyObject().(client.Object)
					replacement.SetOwnerReferences([]metav1.OwnerReference{foreignDispatchOwner(name)})
					if err := c.Update(ctx, replacement); err != nil {
						return err
					}
					return c.Delete(ctx, obj, opts...)
				},
			}).Build()

			err := tt.deleteObj(cl)
			require.Error(t, err)
			preserved := tt.fresh()
			require.NoError(t, cl.Get(context.Background(), tt.key, preserved))
			controller := metav1.GetControllerOf(preserved)
			require.NotNil(t, controller)
			assert.Equal(t, foreignDispatchOwner(name).UID, controller.UID)
		})
	}
}

func TestDispatchAutoscaler_TerminatingRequestedScalerPreservesWorkingSibling(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)

	tests := []struct {
		name       string
		autoscaler *v1beta1.ComponentAutoscaler
		terminate  func(t *testing.T, c client.Client)
		addSibling func(t *testing.T, c client.Client)
		getSibling func(t *testing.T, c client.Client)
	}{
		{
			name:       "HPA to KEDA",
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
			terminate: func(t *testing.T, c client.Client) {
				t.Helper()
				obj := &kedav1.ScaledObject{}
				key := types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}
				require.NoError(t, c.Get(context.Background(), key, obj))
				obj.Finalizers = []string{"example.com/finalizer"}
				require.NoError(t, c.Update(context.Background(), obj))
				require.NoError(t, c.Delete(context.Background(), obj))
				require.NoError(t, c.Get(context.Background(), key, obj))
				require.False(t, obj.DeletionTimestamp.IsZero())
			},
			addSibling: func(t *testing.T, c client.Client) {
				t.Helper()
				require.NoError(t, c.Create(context.Background(), existingHPA(ns, name, dispatchOwner(name))))
			},
			getSibling: func(t *testing.T, c client.Client) {
				t.Helper()
				require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &autoscalingv2.HorizontalPodAutoscaler{}))
			},
		},
		{
			name:       "KEDA to HPA",
			autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
			terminate: func(t *testing.T, c client.Client) {
				t.Helper()
				obj := &autoscalingv2.HorizontalPodAutoscaler{}
				key := types.NamespacedName{Namespace: ns, Name: name}
				require.NoError(t, c.Get(context.Background(), key, obj))
				obj.Finalizers = []string{"example.com/finalizer"}
				require.NoError(t, c.Update(context.Background(), obj))
				require.NoError(t, c.Delete(context.Background(), obj))
				require.NoError(t, c.Get(context.Background(), key, obj))
				require.False(t, obj.DeletionTimestamp.IsZero())
			},
			addSibling: func(t *testing.T, c client.Client) {
				t.Helper()
				require.NoError(t, c.Create(context.Background(), existingScaledObject(ns, name, dispatchOwner(name))))
			},
			getSibling: func(t *testing.T, c client.Client) {
				t.Helper()
				require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, &kedav1.ScaledObject{}))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := dispatchScheme(t)
			cl := fake.NewClientBuilder().WithScheme(scheme).Build()
			params := DispatchParams{
				Client:         cl,
				Scheme:         scheme,
				Owner:          dispatchOwner(name),
				Namespace:      ns,
				Name:           name,
				ScaleTargetRef: dispatchScaleTargetRef(name),
				Autoscaler:     tt.autoscaler,
				MinReplicas:    1,
				MaxReplicas:    5,
			}
			require.NoError(t, DispatchAutoscaler(context.Background(), params))
			tt.terminate(t, cl)
			tt.addSibling(t, cl)

			err := DispatchAutoscaler(context.Background(), params)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "terminating")
			tt.getSibling(t, cl)
		})
	}
}

// TestDispatchAutoscaler_ClassSwitch_HPAToKEDA exercises the full
// class-switch lifecycle on the SAME fake client: first DispatchAutoscaler
// with class=hpa creates the HPA, the second with class=keda must delete
// that HPA and create the SO. Mirrors the operator workflow of editing
// isvc.spec.engine.autoscaler.class.
func TestDispatchAutoscaler_ClassSwitch_HPAToKEDA(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)
	scheme := dispatchScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	// First reconcile: class=hpa.
	err := DispatchAutoscaler(context.Background(), DispatchParams{
		Client:         cl,
		Scheme:         scheme,
		Owner:          dispatchOwner(name),
		Namespace:      ns,
		Name:           name,
		ScaleTargetRef: dispatchScaleTargetRef(name),
		Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
		MinReplicas:    1,
		MaxReplicas:    5,
	})
	require.NoError(t, err)

	// HPA exists, SO does not.
	hpaExists := func() bool {
		err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &autoscalingv2.HorizontalPodAutoscaler{})
		return err == nil
	}
	soExists := func() bool {
		err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, &kedav1.ScaledObject{})
		return err == nil
	}
	assert.True(t, hpaExists(), "after class=hpa reconcile, HPA should exist")
	assert.False(t, soExists(), "after class=hpa reconcile, SO should not exist")

	// Second reconcile: class=keda — must delete the prior HPA and create the SO.
	err = DispatchAutoscaler(context.Background(), DispatchParams{
		Client:         cl,
		Scheme:         scheme,
		Owner:          dispatchOwner(name),
		Namespace:      ns,
		Name:           name,
		ScaleTargetRef: dispatchScaleTargetRef(name),
		Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
		MinReplicas:    1,
		MaxReplicas:    5,
	})
	require.NoError(t, err)

	assert.False(t, hpaExists(), "after class=keda reconcile, prior HPA should be deleted")
	assert.True(t, soExists(), "after class=keda reconcile, SO should exist")
}

// TestDispatchAutoscaler_ClassSwitch_KEDAToHPA covers the reverse:
// first class=keda creates the SO, second class=hpa must delete the SO
// and create the HPA.
func TestDispatchAutoscaler_ClassSwitch_KEDAToHPA(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)
	scheme := dispatchScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	// First reconcile: class=keda.
	err := DispatchAutoscaler(context.Background(), DispatchParams{
		Client:         cl,
		Scheme:         scheme,
		Owner:          dispatchOwner(name),
		Namespace:      ns,
		Name:           name,
		ScaleTargetRef: dispatchScaleTargetRef(name),
		Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: kedaAutoscalerBlock()},
		MinReplicas:    1,
		MaxReplicas:    5,
	})
	require.NoError(t, err)

	hpaExists := func() bool {
		err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &autoscalingv2.HorizontalPodAutoscaler{})
		return err == nil
	}
	soExists := func() bool {
		err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: utils.GetScaledObjectName(name)}, &kedav1.ScaledObject{})
		return err == nil
	}
	assert.False(t, hpaExists(), "after class=keda reconcile, HPA should not exist")
	assert.True(t, soExists(), "after class=keda reconcile, SO should exist")

	// Second reconcile: class=hpa — must delete the prior SO and create the HPA.
	err = DispatchAutoscaler(context.Background(), DispatchParams{
		Client:         cl,
		Scheme:         scheme,
		Owner:          dispatchOwner(name),
		Namespace:      ns,
		Name:           name,
		ScaleTargetRef: dispatchScaleTargetRef(name),
		Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA},
		MinReplicas:    1,
		MaxReplicas:    5,
	})
	require.NoError(t, err)

	assert.True(t, hpaExists(), "after class=hpa reconcile, HPA should exist")
	assert.False(t, soExists(), "after class=hpa reconcile, prior SO should be deleted")
}

// TestDispatchAutoscaler_HPA_DefaultMetric verifies that when the
// resolved Autoscaler has Class=hpa but HPA=nil (the resolver's default
// branch), the generated HPA carries the default of a single CPU=80%
// Resource metric.
func TestDispatchAutoscaler_HPA_DefaultMetric(t *testing.T) {
	const (
		ns   = "test-ns"
		name = "demo-engine"
	)
	scheme := dispatchScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	err := DispatchAutoscaler(context.Background(), DispatchParams{
		Client:         cl,
		Scheme:         scheme,
		Owner:          dispatchOwner(name),
		Namespace:      ns,
		Name:           name,
		ScaleTargetRef: dispatchScaleTargetRef(name),
		Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}, // HPA: nil → default CPU=80%
		MinReplicas:    1,
		MaxReplicas:    5,
	})
	require.NoError(t, err)

	got := &autoscalingv2.HorizontalPodAutoscaler{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, got))

	require.Len(t, got.Spec.Metrics, 1, "default metric list should have exactly one entry")
	require.Equal(t, autoscalingv2.ResourceMetricSourceType, got.Spec.Metrics[0].Type)
	require.NotNil(t, got.Spec.Metrics[0].Resource)
	assert.EqualValues(t, "cpu", got.Spec.Metrics[0].Resource.Name)
	assert.Equal(t, autoscalingv2.UtilizationMetricType, got.Spec.Metrics[0].Resource.Target.Type)
	require.NotNil(t, got.Spec.Metrics[0].Resource.Target.AverageUtilization)
	assert.Equal(t, int32(80), *got.Spec.Metrics[0].Resource.Target.AverageUtilization)
}

// TestDispatchAutoscaler_GuardErrors enforces the input-validation
// contract: nil client, empty owner UID, empty namespace, and empty
// name all return wrapped errors before any cluster I/O. The IR-managed
// dispatch path captures the IR's UID from EnsureInferenceReplica's
// return value; refusing to dispatch on an empty UID guards against
// the dispatch site running before the IR is committed.
func TestDispatchAutoscaler_GuardErrors(t *testing.T) {
	scheme := dispatchScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	validOwner := dispatchOwner("demo-engine")
	validRef := dispatchScaleTargetRef("demo-engine")

	cases := []struct {
		name string
		p    DispatchParams
		want string
	}{
		{
			name: "nil client",
			p:    DispatchParams{Owner: validOwner, Namespace: "ns", Name: "x", ScaleTargetRef: validRef},
			want: "nil client",
		},
		{
			name: "empty owner UID",
			p:    DispatchParams{Client: cl, Owner: metav1.OwnerReference{Name: "x"}, Namespace: "ns", Name: "x", ScaleTargetRef: validRef},
			want: "empty Owner.UID",
		},
		{
			name: "empty namespace",
			p:    DispatchParams{Client: cl, Owner: validOwner, Namespace: "", Name: "x", ScaleTargetRef: validRef},
			want: "empty namespace",
		},
		{
			name: "empty name",
			p:    DispatchParams{Client: cl, Owner: validOwner, Namespace: "ns", Name: "", ScaleTargetRef: validRef},
			want: "empty namespace or name",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := DispatchAutoscaler(context.Background(), tc.p)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}
