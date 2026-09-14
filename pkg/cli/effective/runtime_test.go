package effective

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	sigsyaml "sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeinheritance"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

type selectorStub struct {
	get           func(context.Context, string, string, string) (*v1beta1.ServingRuntimeSpec, bool, error)
	selectRuntime func(context.Context, *v1beta1.BaseModelSpec, *v1beta1.InferenceService) (*runtimeselector.RuntimeSelection, error)
	validate      func(context.Context, string, *v1beta1.BaseModelSpec, *v1beta1.InferenceService) error
}

type getErrorClient struct {
	ctrlclient.Client
	err error
}

func (c getErrorClient) Get(context.Context, ctrlclient.ObjectKey, ctrlclient.Object, ...ctrlclient.GetOption) error {
	return c.err
}

type keyedGetErrorClient struct {
	ctrlclient.Client
	key ctrlclient.ObjectKey
	err error
}

func (c keyedGetErrorClient) Get(
	ctx context.Context,
	key ctrlclient.ObjectKey,
	object ctrlclient.Object,
	options ...ctrlclient.GetOption,
) error {
	if key == c.key {
		return c.err
	}
	return c.Client.Get(ctx, key, object, options...)
}

func (s selectorStub) GetRuntime(ctx context.Context, name, namespace, kind string) (*v1beta1.ServingRuntimeSpec, bool, error) {
	if s.get == nil {
		return nil, false, errors.New("unexpected GetRuntime call")
	}
	return s.get(ctx, name, namespace, kind)
}

func (s selectorStub) SelectRuntime(ctx context.Context, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService) (*runtimeselector.RuntimeSelection, error) {
	if s.selectRuntime == nil {
		return nil, errors.New("unexpected SelectRuntime call")
	}
	return s.selectRuntime(ctx, model, isvc)
}

func (s selectorStub) ValidateRuntime(ctx context.Context, name string, model *v1beta1.BaseModelSpec, isvc *v1beta1.InferenceService) error {
	if s.validate == nil {
		return errors.New("unexpected ValidateRuntime call")
	}
	return s.validate(ctx, name, model, isvc)
}

func targetScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	return scheme
}

func runtimeSourceNames(references []RuntimeSourceReference) []string {
	names := make([]string, 0, len(references))
	for _, reference := range references {
		names = append(names, reference.Name)
	}
	return names
}

func TestResolveLiveExplicitRuntimeMergesInheritanceAndISVC(t *testing.T) {
	parent := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "base", UID: "base-uid", Generation: 3, ResourceVersion: "30"},
		Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{
			Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
				Name:  "runner",
				Image: "base:image",
				Env:   []corev1.EnvVar{{Name: "FROM_PARENT", Value: "yes"}},
			}},
		}},
	}
	child := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "team-runtime",
			Namespace:       "workloads",
			UID:             "runtime-uid",
			Generation:      5,
			ResourceVersion: "50",
			Annotations:     map[string]string{constants.RuntimeInheritFromAnnotationKey: "base"},
		},
		Spec: v1beta1.ServingRuntimeSpec{},
	}
	client := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(parent, child).Build()
	selector := runtimeselector.New(client)
	resolver := newRuntimeResolver(client, selector)
	mode := constants.OMENative
	autoSync := false
	revision := "team-runtime-revision"
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"},
		Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: &mode,
			Runtime: &v1beta1.ServingRuntimeRef{
				Name:     "team-runtime",
				Kind:     ptr.To(runtimeselector.KindServingRuntime),
				AutoSync: &autoSync,
				Revision: &revision,
			},
			Engine: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "override:image"}}},
		},
	}

	got, err := resolver.ResolveLive(context.Background(), isvc)
	require.NoError(t, err)
	assert.Equal(t, RuntimeExplicit, got.Runtime.SelectionSource)
	assert.Equal(t, "team-runtime", got.Runtime.Name)
	assert.Equal(t, runtimeselector.KindServingRuntime, got.Runtime.Kind)
	assert.Equal(t, runtimeselector.KindServingRuntime, got.Runtime.RequestedKind)
	assert.True(t, got.Runtime.RequestedKindSet)
	assert.Equal(t, RuntimePinSource{
		State: RuntimePinSourceResolved, Kind: runtimeselector.KindServingRuntime, Namespace: "workloads",
	}, got.Runtime.PinSource)
	assert.Equal(t, "workloads", got.Runtime.Namespace)
	assert.Equal(t, InheritanceObserved, got.Runtime.DeclaredInheritance.State())
	assert.Equal(t, []string{"base", "team-runtime"}, runtimeSourceNames(got.Runtime.DeclaredInheritance.Chain()))
	assert.Equal(t, []RuntimeSourceReference{
		{
			APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: runtimeselector.KindClusterServingRuntime,
			Name: "base", UID: "base-uid", Generation: 3, ResourceVersion: "30",
		},
		{
			APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: runtimeselector.KindServingRuntime,
			Namespace: "workloads", Name: "team-runtime", UID: "runtime-uid", Generation: 5, ResourceVersion: "50",
		},
	}, got.Runtime.DeclaredInheritance.Chain())
	assert.Empty(t, got.Runtime.DeclaredInheritance.UnavailableReason())
	assert.False(t, got.Runtime.AutoSync)
	assert.Equal(t, "team-runtime-revision", got.Runtime.RequestedRevision)
	require.Len(t, got.Components, 1)
	assert.Equal(t, v1beta1.EngineComponent, got.Components[0].Type)
	assert.Equal(t, constants.OMENative, got.Components[0].DeploymentMode)
	require.NotNil(t, got.Components[0].engine)
	assert.Equal(t, "override:image", got.Components[0].engine.Runner.Image)
	assert.Equal(t, "runner", got.Components[0].engine.Runner.Name)
	assert.Equal(t, []corev1.EnvVar{{Name: "FROM_PARENT", Value: "yes"}}, got.Components[0].engine.Runner.Env)

	// Resolution and merging must not modify source API objects.
	assert.Equal(t, "base:image", parent.Spec.EngineConfig.Runner.Image)
	assert.Empty(t, isvc.Spec.Engine.Runner.Name)
}

func TestResolveLiveAutoSelectsAfterResolvingModel(t *testing.T) {
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "workloads"},
		Spec:       v1beta1.BaseModelSpec{},
	}
	parent := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "base-runtime"},
		Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{
			Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "parent-runner", Image: "parent:image"}},
		}},
	}
	runtimeObject := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "cluster-runtime",
			Annotations: map[string]string{constants.RuntimeInheritFromAnnotationKey: parent.Name},
		},
		Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{
			Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "live-runner", Image: "live:image"}},
		}},
	}
	selectionSpec := &v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{
		Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "selected-runner", Image: "selected:image"}},
	}}
	client := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(model, parent, runtimeObject).Build()
	selector := selectorStub{selectRuntime: func(_ context.Context, gotModel *v1beta1.BaseModelSpec, gotISVC *v1beta1.InferenceService) (*runtimeselector.RuntimeSelection, error) {
		assert.Equal(t, &model.Spec, gotModel)
		assert.Equal(t, "chat", gotISVC.Name)
		return &runtimeselector.RuntimeSelection{Name: "cluster-runtime", IsCluster: true, Spec: selectionSpec}, nil
	}}
	resolver := newRuntimeResolver(client, selector)
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"},
		Spec: v1beta1.InferenceServiceSpec{
			Model:  &v1beta1.ModelRef{Name: "llama"},
			Engine: &v1beta1.EngineSpec{},
		},
	}

	got, err := resolver.ResolveLive(context.Background(), isvc)
	require.NoError(t, err)
	require.NotNil(t, got.Model)
	assert.Equal(t, "llama", got.Model.Name)
	assert.Equal(t, "BaseModel", got.Model.Kind)
	assert.Equal(t, "workloads", got.Model.Namespace)
	assert.Equal(t, RuntimeSelected, got.Runtime.SelectionSource)
	assert.Empty(t, got.Runtime.RequestedKind)
	assert.False(t, got.Runtime.RequestedKindSet)
	assert.Equal(t, RuntimePinSource{State: RuntimePinSourceNotApplicable}, got.Runtime.PinSource)
	assert.Equal(t, runtimeselector.KindClusterServingRuntime, got.Runtime.Kind)
	assert.Empty(t, got.Runtime.Namespace)
	assert.Equal(t, InheritanceObserved, got.Runtime.DeclaredInheritance.State())
	assert.Equal(t, []string{"base-runtime", "cluster-runtime"}, runtimeSourceNames(got.Runtime.DeclaredInheritance.Chain()))
	assert.Empty(t, got.Runtime.DeclaredInheritance.UnavailableReason())
	require.NotNil(t, got.Runtime.spec.EngineConfig.Runner)
	assert.Equal(t, "selected:image", got.Runtime.spec.EngineConfig.Runner.Image)
	require.Len(t, got.Components, 1)
	assert.Equal(t, constants.RawDeployment, got.Components[0].DeploymentMode)
	require.NotNil(t, got.Components[0].engine.Runner)
	assert.Equal(t, "selected:image", got.Components[0].engine.Runner.Image)
	assert.Equal(t, "selected-runner", got.Components[0].engine.Runner.Name)
	assert.NotNil(t, got.Advisories)

	got.Runtime.spec.EngineConfig.Runner.Image = "mutated-runtime:image"
	got.Components[0].engine.Runner.Image = "mutated-component:image"
	assert.Equal(t, "selected:image", selectionSpec.EngineConfig.Runner.Image)
}

func TestResolveLiveExplicitUsesGetRuntimeSnapshot(t *testing.T) {
	liveObject := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "runtime"},
		Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{
			Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "later-live:image"}},
		}},
	}
	authoritativeSpec := &v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{
		Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "get-runtime-snapshot:image"}},
	}}
	client := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(liveObject).Build()
	resolver := newRuntimeResolver(client, selectorStub{get: func(context.Context, string, string, string) (*v1beta1.ServingRuntimeSpec, bool, error) {
		return authoritativeSpec, true, nil
	}})
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: liveObject.Name},
			Engine:  &v1beta1.EngineSpec{},
		},
	}

	got, err := resolver.ResolveLive(context.Background(), isvc)
	require.NoError(t, err)
	assert.Equal(t, "get-runtime-snapshot:image", got.Runtime.spec.EngineConfig.Runner.Image)
	assert.Equal(t, "get-runtime-snapshot:image", got.Components[0].engine.Runner.Image)
	assert.Equal(t, InheritanceObserved, got.Runtime.DeclaredInheritance.State())
	assert.Equal(t, []string{"runtime"}, runtimeSourceNames(got.Runtime.DeclaredInheritance.Chain()))
}

func TestResolveLiveKeepsSelectedSnapshotWhenInheritanceCannotBeObserved(t *testing.T) {
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "workloads"},
		Spec:       v1beta1.BaseModelSpec{},
	}
	selectedSpec := &v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{
		Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "selected:image"}},
	}}
	client := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(model).Build()
	resolver := newRuntimeResolver(client, selectorStub{selectRuntime: func(context.Context, *v1beta1.BaseModelSpec, *v1beta1.InferenceService) (*runtimeselector.RuntimeSelection, error) {
		return &runtimeselector.RuntimeSelection{Name: "deleted-runtime", IsCluster: true, Spec: selectedSpec}, nil
	}})
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"},
		Spec: v1beta1.InferenceServiceSpec{
			Model:  &v1beta1.ModelRef{Name: model.Name},
			Engine: &v1beta1.EngineSpec{},
		},
	}

	got, err := resolver.ResolveLive(context.Background(), isvc)
	require.NoError(t, err)
	assert.Equal(t, InheritanceUnavailable, got.Runtime.DeclaredInheritance.State())
	assert.Equal(t, InheritanceNotFound, got.Runtime.DeclaredInheritance.UnavailableReason())
	assert.Empty(t, got.Runtime.DeclaredInheritance.Chain())
	assert.Equal(t, "selected:image", got.Components[0].engine.Runner.Image)
}

func TestResolveLiveClassifiesForbiddenInheritanceObservation(t *testing.T) {
	base := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).Build()
	client := getErrorClient{
		Client: base,
		err: apierrors.NewForbidden(
			schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "clusterservingruntimes"},
			"runtime",
			errors.New("denied"),
		),
	}
	resolver := newRuntimeResolver(client, selectorStub{get: func(context.Context, string, string, string) (*v1beta1.ServingRuntimeSpec, bool, error) {
		return &v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}}, true, nil
	}})
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: "runtime"},
			Engine:  &v1beta1.EngineSpec{},
		},
	}

	got, err := resolver.ResolveLive(context.Background(), isvc)
	require.NoError(t, err)
	assert.Equal(t, InheritanceUnavailable, got.Runtime.DeclaredInheritance.State())
	assert.Equal(t, InheritanceForbidden, got.Runtime.DeclaredInheritance.UnavailableReason())
	assert.Empty(t, got.Runtime.DeclaredInheritance.Chain())
}

func TestResolveLivePropagatesCanceledInheritanceObservation(t *testing.T) {
	base := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).Build()
	client := getErrorClient{Client: base, err: context.Canceled}
	resolver := newRuntimeResolver(client, selectorStub{get: func(context.Context, string, string, string) (*v1beta1.ServingRuntimeSpec, bool, error) {
		return &v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}}, true, nil
	}})
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: "runtime"},
			Engine:  &v1beta1.EngineSpec{},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, err := resolver.ResolveLive(ctx, isvc)
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, got)
}

func TestObserveDeclaredInheritanceTracksNamespacedParents(t *testing.T) {
	parent := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "base", Namespace: "workloads", UID: "base-uid", ResourceVersion: "base-rv"},
		Spec:       v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}},
	}
	child := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "child", Namespace: "workloads", UID: "child-uid", ResourceVersion: "child-rv",
			Annotations: map[string]string{constants.RuntimeInheritFromAnnotationKey: parent.Name},
		},
		Spec: v1beta1.ServingRuntimeSpec{},
	}
	client := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(parent, child).Build()

	got, err := observeDeclaredInheritance(context.Background(), client, "workloads", child.Name, false)
	require.NoError(t, err)
	require.NoError(t, got.Validate())
	assert.Equal(t, []RuntimeSourceReference{
		{
			APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: runtimeselector.KindServingRuntime,
			Namespace: "workloads", Name: parent.Name, UID: "base-uid", ResourceVersion: "base-rv",
		},
		{
			APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: runtimeselector.KindServingRuntime,
			Namespace: "workloads", Name: child.Name, UID: "child-uid", ResourceVersion: "child-rv",
		},
	}, got.Chain())
}

func TestObserveDeclaredInheritanceClassifiesNamespacedReadFailures(t *testing.T) {
	base := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).Build()
	notFound, err := observeDeclaredInheritance(context.Background(), base, "workloads", "missing", false)
	require.NoError(t, err)
	assert.Equal(t, InheritanceNotFound, notFound.UnavailableReason())

	deniedClient := getErrorClient{
		Client: base,
		err: apierrors.NewForbidden(
			schema.GroupResource{Resource: "servingruntimes"}, "runtime", errors.New("denied"),
		),
	}
	forbidden, err := observeDeclaredInheritance(context.Background(), deniedClient, "workloads", "runtime", false)
	require.NoError(t, err)
	assert.Equal(t, InheritanceForbidden, forbidden.UnavailableReason())
}

func TestObserveDeclaredInheritanceClassifiesParentReadFailure(t *testing.T) {
	child := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "child", Namespace: "workloads",
			Annotations: map[string]string{constants.RuntimeInheritFromAnnotationKey: "parent"},
		},
		Spec: v1beta1.ServingRuntimeSpec{},
	}
	base := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(child).Build()
	client := keyedGetErrorClient{
		Client: base,
		key:    ctrlclient.ObjectKey{Name: "parent", Namespace: "workloads"},
		err:    errors.New("transport failed with redaction-canary"),
	}

	got, err := observeDeclaredInheritance(context.Background(), client, "workloads", child.Name, false)
	require.NoError(t, err)
	assert.Equal(t, InheritanceUnreadable, got.UnavailableReason())
}

func TestResolveLiveMirrorsExplicitRuntimeValidation(t *testing.T) {
	compatibilityErr := &runtimeselector.RuntimeCompatibilityError{
		RuntimeName: "runtime", ModelName: "llama", ModelFormat: "safetensors", Reason: "untrusted raw mismatch details",
	}
	hardErr := &runtimeselector.ModelValidationError{Field: "modelFormat.name", Message: "required"}
	runtimeSpec := &v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}}

	tests := []struct {
		name         string
		distribution *v1beta1.Distribution
		autoSync     *bool
		validateErr  error
		wantErr      error
		wantAdvisory bool
		wantValidate bool
	}{
		{name: "valid live runtime", validateErr: nil, wantValidate: true},
		{name: "non-sharded compatibility advisory", validateErr: compatibilityErr, wantAdvisory: true, wantValidate: true},
		{name: "sharded compatibility hard failure", distribution: ptr.To(v1beta1.DistributionSharded), validateErr: compatibilityErr, wantErr: compatibilityErr, wantValidate: true},
		{name: "malformed model hard failure", validateErr: hardErr, wantErr: hardErr, wantValidate: true},
		{name: "pinned runtime skips live validation", autoSync: ptr.To(false), validateErr: hardErr},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "workloads"},
				Spec:       v1beta1.BaseModelSpec{Distribution: test.distribution},
			}
			client := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(model).Build()
			validated := false
			resolver := newRuntimeResolver(client, selectorStub{
				validate: func(_ context.Context, name string, gotModel *v1beta1.BaseModelSpec, _ *v1beta1.InferenceService) error {
					validated = true
					assert.Equal(t, "runtime", name)
					assert.Equal(t, &model.Spec, gotModel)
					return test.validateErr
				},
				get: func(context.Context, string, string, string) (*v1beta1.ServingRuntimeSpec, bool, error) {
					return runtimeSpec, true, nil
				},
			})
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"},
				Spec: v1beta1.InferenceServiceSpec{
					Model:   &v1beta1.ModelRef{Name: model.Name},
					Runtime: &v1beta1.ServingRuntimeRef{Name: "runtime", AutoSync: test.autoSync},
					Engine:  &v1beta1.EngineSpec{},
				},
			}

			got, err := resolver.ResolveLive(context.Background(), isvc)
			assert.Equal(t, test.wantValidate, validated)
			if test.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, test.wantErr)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			if test.wantAdvisory {
				require.Equal(t, []RuntimeAdvisory{{Code: RuntimeAdvisoryDeclaredCompatibilityMismatch}}, got.Advisories)
			} else {
				assert.Empty(t, got.Advisories)
			}
		})
	}
}

func TestResolveLiveUsesActualScopeAfterDefaultKindFallback(t *testing.T) {
	autoSync := false
	tests := []struct {
		name             string
		kind             *string
		wantRequested    string
		wantRequestedSet bool
	}{
		{name: "nil kind", wantRequestedSet: false},
		{
			name: "declared cluster kind", kind: ptr.To(runtimeselector.KindClusterServingRuntime),
			wantRequested: runtimeselector.KindClusterServingRuntime, wantRequestedSet: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtimeObject := &v1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{Name: "fallback", Namespace: "workloads"},
				Spec:       v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}},
			}
			client := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(runtimeObject).Build()
			resolver := NewRuntimeResolver(client)
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{Name: "fallback", Kind: test.kind, AutoSync: &autoSync},
					Engine:  &v1beta1.EngineSpec{},
				},
			}

			got, err := resolver.ResolveLive(context.Background(), isvc)
			require.NoError(t, err)
			assert.Equal(t, runtimeselector.KindServingRuntime, got.Runtime.Kind)
			assert.Equal(t, "workloads", got.Runtime.Namespace)
			assert.Equal(t, test.wantRequested, got.Runtime.RequestedKind)
			assert.Equal(t, test.wantRequestedSet, got.Runtime.RequestedKindSet)
			assert.Equal(t, RuntimePinSource{
				State: RuntimePinSourceResolved, Kind: runtimeselector.KindClusterServingRuntime,
			}, got.Runtime.PinSource)
		})
	}
}

func TestRuntimeReferenceIntentPreservesPinSignificantKindState(t *testing.T) {
	autoSyncFalse := false
	autoSyncTrue := true
	emptyKind := ""
	unknownKind := "UnknownRuntime"
	tests := []struct {
		name          string
		source        RuntimeSelectionSource
		reference     *v1beta1.ServingRuntimeRef
		wantAutoSync  bool
		wantKind      string
		wantKindSet   bool
		wantRevision  string
		wantPinSource RuntimePinSource
	}{
		{
			name: "selected ignores empty-name runtime options", source: RuntimeSelected,
			reference:    &v1beta1.ServingRuntimeRef{Name: "", Kind: ptr.To(runtimeselector.KindServingRuntime), AutoSync: &autoSyncFalse, Revision: ptr.To("ignored")},
			wantAutoSync: true, wantPinSource: RuntimePinSource{State: RuntimePinSourceNotApplicable},
		},
		{
			name: "nil kind pins cluster source", source: RuntimeExplicit,
			reference:     &v1beta1.ServingRuntimeRef{Name: "runtime", AutoSync: &autoSyncFalse},
			wantPinSource: RuntimePinSource{State: RuntimePinSourceResolved, Kind: runtimeselector.KindClusterServingRuntime},
		},
		{
			name: "cluster kind pins cluster source", source: RuntimeExplicit,
			reference: &v1beta1.ServingRuntimeRef{Name: "runtime", Kind: ptr.To(runtimeselector.KindClusterServingRuntime), AutoSync: &autoSyncFalse},
			wantKind:  runtimeselector.KindClusterServingRuntime, wantKindSet: true,
			wantPinSource: RuntimePinSource{State: RuntimePinSourceResolved, Kind: runtimeselector.KindClusterServingRuntime},
		},
		{
			name: "serving kind pins workload namespace", source: RuntimeExplicit,
			reference: &v1beta1.ServingRuntimeRef{Name: "runtime", Kind: ptr.To(runtimeselector.KindServingRuntime), AutoSync: &autoSyncFalse, Revision: ptr.To("revision-1")},
			wantKind:  runtimeselector.KindServingRuntime, wantKindSet: true, wantRevision: "revision-1",
			wantPinSource: RuntimePinSource{State: RuntimePinSourceResolved, Kind: runtimeselector.KindServingRuntime, Namespace: "workloads"},
		},
		{
			name: "pointer-to-empty kind is invalid", source: RuntimeExplicit,
			reference:   &v1beta1.ServingRuntimeRef{Name: "runtime", Kind: &emptyKind, AutoSync: &autoSyncFalse},
			wantKindSet: true, wantPinSource: RuntimePinSource{State: RuntimePinSourceInvalid},
		},
		{
			name: "unknown kind is invalid", source: RuntimeExplicit,
			reference: &v1beta1.ServingRuntimeRef{Name: "runtime", Kind: &unknownKind, AutoSync: &autoSyncFalse},
			wantKind:  unknownKind, wantKindSet: true, wantPinSource: RuntimePinSource{State: RuntimePinSourceInvalid},
		},
		{
			name: "auto sync has no active pin source", source: RuntimeExplicit,
			reference:    &v1beta1.ServingRuntimeRef{Name: "runtime", Kind: ptr.To(runtimeselector.KindServingRuntime), AutoSync: &autoSyncTrue, Revision: ptr.To("ignored-live-revision")},
			wantAutoSync: true, wantKind: runtimeselector.KindServingRuntime, wantKindSet: true,
			wantRevision: "ignored-live-revision", wantPinSource: RuntimePinSource{State: RuntimePinSourceNotApplicable},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Namespace: "workloads"},
				Spec:       v1beta1.InferenceServiceSpec{Runtime: test.reference},
			}
			autoSync, kind, kindSet, revision, pinSource := runtimeReferenceIntent(isvc, test.source)
			assert.Equal(t, test.wantAutoSync, autoSync)
			assert.Equal(t, test.wantKind, kind)
			assert.Equal(t, test.wantKindSet, kindSet)
			assert.Equal(t, test.wantRevision, revision)
			assert.Equal(t, test.wantPinSource, pinSource)
			require.NoError(t, pinSource.Validate())
		})
	}
}

func TestRuntimePinSourceValidation(t *testing.T) {
	valid := []RuntimePinSource{
		{State: RuntimePinSourceNotApplicable},
		{State: RuntimePinSourceInvalid},
		{State: RuntimePinSourceResolved, Kind: runtimeselector.KindClusterServingRuntime},
		{State: RuntimePinSourceResolved, Kind: runtimeselector.KindServingRuntime, Namespace: "workloads"},
	}
	for _, source := range valid {
		require.NoError(t, source.Validate())
	}

	invalid := []RuntimePinSource{
		{},
		{State: "Unknown"},
		{State: RuntimePinSourceNotApplicable, Kind: runtimeselector.KindServingRuntime},
		{State: RuntimePinSourceInvalid, Namespace: "workloads"},
		{State: RuntimePinSourceResolved},
		{State: RuntimePinSourceResolved, Kind: runtimeselector.KindClusterServingRuntime, Namespace: "workloads"},
		{State: RuntimePinSourceResolved, Kind: runtimeselector.KindServingRuntime},
		{State: RuntimePinSourceResolved, Kind: "UnknownRuntime"},
	}
	for _, source := range invalid {
		require.Error(t, source.Validate())
	}
}

func TestResolveLiveIgnoresEmptyNameRuntimeOptionsDuringSelection(t *testing.T) {
	autoSync := false
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "workloads"},
		Spec:       v1beta1.BaseModelSpec{},
	}
	runtimeObject := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "selected"},
		Spec:       v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}},
	}
	client := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(model, runtimeObject).Build()
	resolver := newRuntimeResolver(client, selectorStub{selectRuntime: func(context.Context, *v1beta1.BaseModelSpec, *v1beta1.InferenceService) (*runtimeselector.RuntimeSelection, error) {
		return &runtimeselector.RuntimeSelection{Name: runtimeObject.Name, IsCluster: true, Spec: runtimeObject.Spec.DeepCopy()}, nil
	}})
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"},
		Spec: v1beta1.InferenceServiceSpec{
			Model: &v1beta1.ModelRef{Name: model.Name},
			Runtime: &v1beta1.ServingRuntimeRef{
				Kind: ptr.To(runtimeselector.KindServingRuntime), AutoSync: &autoSync, Revision: ptr.To("ignored"),
			},
			Engine: &v1beta1.EngineSpec{},
		},
	}

	got, err := resolver.ResolveLive(context.Background(), isvc)
	require.NoError(t, err)
	assert.Equal(t, RuntimeSelected, got.Runtime.SelectionSource)
	assert.True(t, got.Runtime.AutoSync)
	assert.False(t, got.Runtime.RequestedKindSet)
	assert.Empty(t, got.Runtime.RequestedRevision)
	assert.Equal(t, RuntimePinSource{State: RuntimePinSourceNotApplicable}, got.Runtime.PinSource)
}

func TestResolveLiveReportsInvalidDeclaredRuntimeKind(t *testing.T) {
	autoSync := false
	runtimeObject := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "runtime"},
		Spec:       v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}},
	}
	client := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(runtimeObject).Build()
	for _, kind := range []string{"", "UnknownRuntime"} {
		t.Run(kind, func(t *testing.T) {
			resolver := newRuntimeResolver(client, selectorStub{get: func(context.Context, string, string, string) (*v1beta1.ServingRuntimeSpec, bool, error) {
				return runtimeObject.Spec.DeepCopy(), true, nil
			}})
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{Name: runtimeObject.Name, Kind: &kind, AutoSync: &autoSync},
					Engine:  &v1beta1.EngineSpec{},
				},
			}

			got, err := resolver.ResolveLive(context.Background(), isvc)
			require.NoError(t, err)
			assert.Equal(t, RuntimePinSource{State: RuntimePinSourceInvalid}, got.Runtime.PinSource)
			assert.Equal(t, []RuntimeAdvisory{{Code: RuntimeAdvisoryInvalidDeclaredKind}}, got.Advisories)
		})
	}
}

func TestMergeEffectiveComponentsReportsDeploymentModeSource(t *testing.T) {
	omeNative := constants.OMENative
	tests := []struct {
		name               string
		serviceAnnotations map[string]string
		engine             *v1beta1.EngineSpec
		specMode           *constants.DeploymentModeType
		wantMode           constants.DeploymentModeType
		wantSource         ComponentDeploymentModeSource
	}{
		{
			name: "service virtual annotation is the controller early-exit mode",
			serviceAnnotations: map[string]string{
				constants.DeploymentMode: string(constants.VirtualDeployment),
			},
			engine: &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				Annotations: map[string]string{constants.DeploymentMode: string(constants.RawDeployment)},
			}},
			specMode: &omeNative, wantMode: constants.VirtualDeployment,
			wantSource: DeploymentModeServiceAnnotation,
		},
		{
			name: "component annotation",
			engine: &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				Annotations: map[string]string{constants.DeploymentMode: string(constants.RawDeployment)},
			}},
			specMode:   &omeNative,
			wantMode:   constants.RawDeployment,
			wantSource: DeploymentModeComponentAnnotation,
		},
		{
			name:       "service spec",
			engine:     &v1beta1.EngineSpec{},
			specMode:   &omeNative,
			wantMode:   constants.OMENative,
			wantSource: DeploymentModeServiceSpec,
		},
		{
			name:       "leader worker shape",
			engine:     &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}},
			wantMode:   constants.OMENative,
			wantSource: DeploymentModeLeaderWorkerShape,
		},
		{
			name:       "default",
			engine:     &v1beta1.EngineSpec{},
			wantMode:   constants.RawDeployment,
			wantSource: DeploymentModeDefault,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: test.serviceAnnotations}, Spec: v1beta1.InferenceServiceSpec{
				DeploymentMode: test.specMode,
				Engine:         test.engine,
			}}
			got, err := MergeEffectiveComponents(isvc, &v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}})
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, test.wantMode, got[0].DeploymentMode)
			assert.Equal(t, test.wantSource, got[0].DeploymentModeSource)
		})
	}
}

func TestResolveLiveFallsBackToClusterModel(t *testing.T) {
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama"},
		Spec:       v1beta1.BaseModelSpec{},
	}
	runtimeObject := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-runtime"},
		Spec:       v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}},
	}
	client := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(model, runtimeObject).Build()
	resolver := newRuntimeResolver(client, selectorStub{selectRuntime: func(_ context.Context, gotModel *v1beta1.BaseModelSpec, _ *v1beta1.InferenceService) (*runtimeselector.RuntimeSelection, error) {
		assert.Equal(t, &model.Spec, gotModel)
		return &runtimeselector.RuntimeSelection{Name: runtimeObject.Name, IsCluster: true, Spec: runtimeObject.Spec.DeepCopy()}, nil
	}})
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"},
		Spec: v1beta1.InferenceServiceSpec{
			Model:  &v1beta1.ModelRef{Name: "llama"},
			Engine: &v1beta1.EngineSpec{},
		},
	}

	got, err := resolver.ResolveLive(context.Background(), isvc)
	require.NoError(t, err)
	require.NotNil(t, got.Model)
	assert.Equal(t, "ClusterBaseModel", got.Model.Kind)
	assert.Empty(t, got.Model.Namespace)
	assert.True(t, got.Runtime.AutoSync)
	assert.Empty(t, got.Runtime.RequestedRevision)
}

func TestResolveLiveReturnsComponentsInStableOrder(t *testing.T) {
	runtimeObject := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "runtime"},
		Spec: v1beta1.ServingRuntimeSpec{
			EngineConfig:  &v1beta1.EngineSpec{},
			DecoderConfig: &v1beta1.DecoderSpec{},
			RouterConfig:  &v1beta1.RouterSpec{},
		},
	}
	client := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(runtimeObject).Build()
	resolver := newRuntimeResolver(client, runtimeselector.New(client))
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: "runtime"},
			Engine:  &v1beta1.EngineSpec{},
			Decoder: &v1beta1.DecoderSpec{},
			Router:  &v1beta1.RouterSpec{},
		},
	}

	got, err := resolver.ResolveLive(context.Background(), isvc)
	require.NoError(t, err)
	require.Len(t, got.Components, 3)
	assert.Equal(t, []v1beta1.ComponentType{
		v1beta1.EngineComponent,
		v1beta1.DecoderComponent,
		v1beta1.RouterComponent,
	}, []v1beta1.ComponentType{got.Components[0].Type, got.Components[1].Type, got.Components[2].Type})
}

func TestResolveLiveRejectsInvalidOrDisabledInputs(t *testing.T) {
	disabledModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "disabled-model", Namespace: "workloads"},
		Spec: v1beta1.BaseModelSpec{
			ModelExtensionSpec: v1beta1.ModelExtensionSpec{Disabled: ptr.To(true)},
		},
	}
	disabledRuntime := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "disabled-runtime"},
		Spec:       v1beta1.ServingRuntimeSpec{Disabled: ptr.To(true), EngineConfig: &v1beta1.EngineSpec{}},
	}
	client := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(disabledModel, disabledRuntime).Build()
	resolver := newRuntimeResolver(client, runtimeselector.New(client))

	tests := []struct {
		name string
		isvc *v1beta1.InferenceService
		want string
	}{
		{name: "nil target", want: "must not be nil"},
		{name: "missing namespace", isvc: &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat"}}, want: "namespace"},
		{name: "no model or runtime", isvc: &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"}, Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}}}, want: "model or runtime"},
		{name: "disabled model", isvc: &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"}, Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "disabled-model"}, Engine: &v1beta1.EngineSpec{}}}, want: "disabled"},
		{name: "disabled runtime", isvc: &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"}, Spec: v1beta1.InferenceServiceSpec{Runtime: &v1beta1.ServingRuntimeRef{Name: "disabled-runtime"}, Engine: &v1beta1.EngineSpec{}}}, want: "disabled"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := resolver.ResolveLive(context.Background(), test.isvc)
			require.Error(t, err)
			assert.ErrorContains(t, err, test.want)
		})
	}
}

func TestResolveLivePropagatesResolutionFailures(t *testing.T) {
	runtimeObject := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "runtime"},
		Spec:       v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}},
	}
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "workloads"}}
	client := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(runtimeObject, model).Build()

	t.Run("unconfigured resolver", func(t *testing.T) {
		_, err := (&RuntimeResolver{}).ResolveLive(context.Background(), &v1beta1.InferenceService{})
		require.ErrorContains(t, err, "not configured")
	})

	t.Run("explicit lookup", func(t *testing.T) {
		resolver := newRuntimeResolver(client, selectorStub{get: func(context.Context, string, string, string) (*v1beta1.ServingRuntimeSpec, bool, error) {
			return nil, false, errors.New("lookup denied")
		}})
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"},
			Spec:       v1beta1.InferenceServiceSpec{Runtime: &v1beta1.ServingRuntimeRef{Name: "runtime"}, Engine: &v1beta1.EngineSpec{}},
		}
		_, err := resolver.ResolveLive(context.Background(), isvc)
		require.ErrorContains(t, err, "lookup denied")
	})

	t.Run("automatic selection", func(t *testing.T) {
		resolver := newRuntimeResolver(client, selectorStub{selectRuntime: func(context.Context, *v1beta1.BaseModelSpec, *v1beta1.InferenceService) (*runtimeselector.RuntimeSelection, error) {
			return nil, errors.New("selection failed")
		}})
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"},
			Spec:       v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "model"}, Engine: &v1beta1.EngineSpec{}},
		}
		_, err := resolver.ResolveLive(context.Background(), isvc)
		require.ErrorContains(t, err, "selection failed")
	})

	t.Run("empty automatic selection", func(t *testing.T) {
		resolver := newRuntimeResolver(client, selectorStub{selectRuntime: func(context.Context, *v1beta1.BaseModelSpec, *v1beta1.InferenceService) (*runtimeselector.RuntimeSelection, error) {
			return &runtimeselector.RuntimeSelection{}, nil
		}})
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"},
			Spec:       v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "model"}, Engine: &v1beta1.EngineSpec{}},
		}
		_, err := resolver.ResolveLive(context.Background(), isvc)
		require.ErrorContains(t, err, "empty result")
	})

	t.Run("missing model", func(t *testing.T) {
		resolver := newRuntimeResolver(client, selectorStub{})
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"},
			Spec:       v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "missing"}, Engine: &v1beta1.EngineSpec{}},
		}
		_, err := resolver.ResolveLive(context.Background(), isvc)
		require.ErrorContains(t, err, "missing")
	})
}

func TestMergeEffectiveComponentsValidatesInputs(t *testing.T) {
	_, err := MergeEffectiveComponents(nil, &v1beta1.ServingRuntimeSpec{})
	require.ErrorContains(t, err, "must not be nil")

	_, err = MergeEffectiveComponents(&v1beta1.InferenceService{}, nil)
	require.ErrorContains(t, err, "must not be nil")

	_, err = MergeEffectiveComponents(&v1beta1.InferenceService{}, &v1beta1.ServingRuntimeSpec{})
	require.ErrorContains(t, err, "engine component is required")
}

func TestResolveLivePreservesTypedInheritanceErrors(t *testing.T) {
	child := &v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "child",
			Annotations: map[string]string{constants.RuntimeInheritFromAnnotationKey: "missing"},
		},
		Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}},
	}
	client := ctrlclientfake.NewClientBuilder().WithScheme(targetScheme(t)).WithObjects(child).Build()
	resolver := newRuntimeResolver(client, runtimeselector.New(client))
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "workloads"},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: "child"},
			Engine:  &v1beta1.EngineSpec{},
		},
	}

	_, err := resolver.ResolveLive(context.Background(), isvc)
	require.Error(t, err)
	var typed *runtimeinheritance.ParentNotFoundError
	assert.True(t, errors.As(err, &typed))
	assert.ErrorContains(t, err, "missing")
}

func TestResolvedRuntimeTypesRejectSerialization(t *testing.T) {
	const sentinel = "redaction-canary-do-not-emit"
	model := ModelResolution{spec: &v1beta1.BaseModelSpec{
		ModelConfiguration: runtime.RawExtension{Raw: []byte(`{"headers":"redaction-canary-do-not-emit","extensions":"redaction-canary-do-not-emit"}`)},
		AdditionalMetadata: map[string]string{"secret": sentinel},
	}}
	runtimeResolution := RuntimeResolution{spec: &v1beta1.ServingRuntimeSpec{
		EngineConfig: &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: map[string]string{"secret": sentinel}},
			Runner: &v1beta1.RunnerSpec{Container: corev1.Container{
				Args: []string{sentinel}, Env: []corev1.EnvVar{{Name: "SECRET", Value: sentinel}},
			}},
		},
	}}
	component := EffectiveComponent{engine: runtimeResolution.spec.EngineConfig.DeepCopy()}
	values := []any{
		model,
		runtimeResolution,
		component,
		LiveConfiguration{Model: &model, Runtime: runtimeResolution, Components: []EffectiveComponent{component}},
	}
	encoders := map[string]func(any) ([]byte, error){
		"json":            json.Marshal,
		"Kubernetes YAML": sigsyaml.Marshal,
	}
	for _, value := range values {
		for name, encode := range encoders {
			t.Run(name, func(t *testing.T) {
				encoded, err := encode(value)
				require.ErrorIs(t, err, ErrUnsafeRuntimeSerialization)
				assert.False(t, strings.Contains(string(encoded), sentinel))
			})
		}
		yamlMarshaler, ok := value.(interface {
			MarshalYAML() (any, error)
		})
		require.True(t, ok)
		yamlValue, err := yamlMarshaler.MarshalYAML()
		require.ErrorIs(t, err, ErrUnsafeRuntimeSerialization)
		assert.Nil(t, yamlValue)
	}
}

func TestSensitiveResolutionFieldsArePrivate(t *testing.T) {
	tests := []struct {
		typeOf reflect.Type
		fields []string
	}{
		{typeOf: reflect.TypeOf(ModelResolution{}), fields: []string{"spec"}},
		{typeOf: reflect.TypeOf(RuntimeResolution{}), fields: []string{"spec"}},
		{typeOf: reflect.TypeOf(EffectiveComponent{}), fields: []string{"engine", "decoder", "router"}},
	}
	for _, test := range tests {
		for _, fieldName := range test.fields {
			field, found := test.typeOf.FieldByName(fieldName)
			require.True(t, found)
			assert.NotEmpty(t, field.PkgPath, "%s.%s must remain unexported", test.typeOf.Name(), fieldName)
		}
	}
}

func TestInheritanceObservationValidation(t *testing.T) {
	reference := RuntimeSourceReference{Kind: runtimeselector.KindClusterServingRuntime, Name: "runtime"}
	observed := observedInheritance([]RuntimeSourceReference{reference})
	require.NoError(t, observed.Validate())
	returnedChain := observed.Chain()
	returnedChain[0].Name = "mutated"
	assert.Equal(t, "runtime", observed.Chain()[0].Name)
	for _, reason := range []InheritanceUnavailableReason{
		InheritanceNotFound,
		InheritanceForbidden,
		InheritanceCycle,
		InheritanceMaxDepthExceeded,
		InheritanceMalformed,
		InheritanceUnreadable,
	} {
		require.NoError(t, unavailableInheritance(reason).Validate())
	}
	unknownReason := unavailableInheritance("Unknown")
	require.NoError(t, unknownReason.Validate())
	assert.Equal(t, InheritanceUnreadable, unknownReason.UnavailableReason())
	emptyObserved := observedInheritance(nil)
	require.NoError(t, emptyObserved.Validate())
	assert.Equal(t, InheritanceUnavailable, emptyObserved.State())
	assert.Equal(t, InheritanceMalformed, emptyObserved.UnavailableReason())

	invalid := []InheritanceObservation{
		{},
		{state: "Unknown"},
		{state: InheritanceObserved},
		{state: InheritanceObserved, chain: []RuntimeSourceReference{reference}, unavailableReason: InheritanceUnreadable},
		{state: InheritanceUnavailable, chain: []RuntimeSourceReference{reference}, unavailableReason: InheritanceUnreadable},
		{state: InheritanceUnavailable},
		{state: InheritanceUnavailable, unavailableReason: "Unknown"},
	}
	for _, observation := range invalid {
		require.Error(t, observation.Validate())
	}
}

func TestInheritanceReadErrorDoesNotExposeCauseText(t *testing.T) {
	cause := errors.New("redaction-canary-do-not-emit")
	err := &inheritanceReadError{cause: cause}
	assert.Equal(t, "runtime inheritance source read failed", err.Error())
	assert.NotContains(t, err.Error(), cause.Error())
	assert.ErrorIs(t, err, cause)
}

func TestClassifyInheritanceUnavailable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want InheritanceUnavailableReason
	}{
		{
			name: "typed missing parent",
			err:  &runtimeinheritance.ParentNotFoundError{Parent: "base"},
			want: InheritanceNotFound,
		},
		{
			name: "API not found",
			err:  apierrors.NewNotFound(schema.GroupResource{Resource: "servingruntimes"}, "base"),
			want: InheritanceNotFound,
		},
		{
			name: "forbidden",
			err: apierrors.NewForbidden(
				schema.GroupResource{Resource: "servingruntimes"}, "base", errors.New("denied"),
			),
			want: InheritanceForbidden,
		},
		{
			name: "unauthorized",
			err:  apierrors.NewUnauthorized("expired token"),
			want: InheritanceForbidden,
		},
		{
			name: "cycle",
			err:  &runtimeinheritance.CycleError{Cycle: []string{"a", "b", "a"}},
			want: InheritanceCycle,
		},
		{
			name: "maximum depth",
			err:  &runtimeinheritance.MaxDepthExceededError{MaxDepth: 5},
			want: InheritanceMaxDepthExceeded,
		},
		{
			name: "other read failure",
			err:  &inheritanceReadError{cause: errors.New("connection reset")},
			want: InheritanceUnreadable,
		},
		{
			name: "malformed inheritance",
			err:  errors.New("merge failed"),
			want: InheritanceMalformed,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, classifyInheritanceUnavailable(test.err))
		})
	}
}

var _ runtimeSelector = selectorStub{}
