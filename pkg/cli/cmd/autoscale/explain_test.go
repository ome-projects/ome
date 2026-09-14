package autoscale

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/autoscaleprojection"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/paging"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	ometypedv1beta1 "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimerevision"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

func TestExplainCommandContract(t *testing.T) {
	streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{}}
	parent := NewCmd(factory.Static{}, streams)

	cmd, _, err := parent.Find([]string{"explain"})
	require.NoError(t, err)
	require.NotSame(t, parent, cmd)
	assert.Equal(t, "explain INFERENCESERVICE", cmd.Use)
	assert.Equal(t, "Explain effective and reported autoscaling", cmd.Short)
	assert.Contains(t, cmd.Long, "controller-selected active runtime configuration")
	assert.Contains(t, cmd.Long, "does not query child autoscaler or workload objects")
	assert.Contains(t, cmd.Long, "does not prove rollout convergence")

	output := cmd.Flags().Lookup("output")
	require.NotNil(t, output)
	assert.Equal(t, "o", output.Shorthand)
	assert.Equal(t, "table", output.DefValue)
	omeNamespace := cmd.Flags().Lookup("ome-namespace")
	require.NotNil(t, omeNamespace)
	assert.Equal(t, "ome", omeNamespace.DefValue)
	assert.Nil(t, cmd.Flags().Lookup("namespace"), "workload namespace must remain inherited")
}

func TestExplainProductionAcquisitionIsBoundReadOnlyAndUsesOneDeadline(t *testing.T) {
	autoSync := true
	kind := runtimeselector.KindServingRuntime
	isvc := explainISVCFixture()
	isvc.Status.ObservedGeneration = isvc.Generation
	isvc.Spec = omev1beta1.InferenceServiceSpec{
		Runtime: &omev1beta1.ServingRuntimeRef{Name: "runtime", Kind: &kind, AutoSync: &autoSync},
		Engine:  &omev1beta1.EngineSpec{},
	}
	runtimeObject := &omev1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "runtime", Namespace: "prod", UID: types.UID("runtime-uid"),
			ResourceVersion: "29", Generation: 2,
		},
		Spec: omev1beta1.ServingRuntimeSpec{EngineConfig: &omev1beta1.EngineSpec{}},
	}
	omeBase := omefake.NewSimpleClientset(isvc)
	omeClient := &explainDeadlineOMEClient{Interface: omeBase}
	runtimeBase := ctrlfake.NewClientBuilder().WithScheme(explainScheme(t)).WithObjects(runtimeObject).Build()
	var runtimeReads []explainRuntimeRead
	writeAttempt := errors.New("unexpected runtime write")
	runtimeClient := interceptor.NewClient(runtimeBase, interceptor.Funcs{
		Get: func(ctx context.Context, client ctrlclient.WithWatch, key ctrlclient.ObjectKey, object ctrlclient.Object, opts ...ctrlclient.GetOption) error {
			deadline, ok := ctx.Deadline()
			runtimeReads = append(runtimeReads, explainRuntimeRead{
				verb: "get", objectType: fmt.Sprintf("%T", object), key: key, deadline: deadline, hasDeadline: ok,
			})
			return client.Get(ctx, key, object, opts...)
		},
		List: func(ctx context.Context, client ctrlclient.WithWatch, list ctrlclient.ObjectList, opts ...ctrlclient.ListOption) error {
			deadline, ok := ctx.Deadline()
			runtimeReads = append(runtimeReads, explainRuntimeRead{
				verb: "list", objectType: fmt.Sprintf("%T", list), deadline: deadline, hasDeadline: ok,
			})
			return client.List(ctx, list, opts...)
		},
		Create: func(context.Context, ctrlclient.WithWatch, ctrlclient.Object, ...ctrlclient.CreateOption) error {
			return writeAttempt
		},
		Delete: func(context.Context, ctrlclient.WithWatch, ctrlclient.Object, ...ctrlclient.DeleteOption) error {
			return writeAttempt
		},
		Update: func(context.Context, ctrlclient.WithWatch, ctrlclient.Object, ...ctrlclient.UpdateOption) error {
			return writeAttempt
		},
		Patch: func(context.Context, ctrlclient.WithWatch, ctrlclient.Object, ctrlclient.Patch, ...ctrlclient.PatchOption) error {
			return writeAttempt
		},
		DeleteAllOf: func(context.Context, ctrlclient.WithWatch, ctrlclient.Object, ...ctrlclient.DeleteAllOfOption) error {
			return writeAttempt
		},
	})
	kubeClient := k8sfake.NewSimpleClientset()
	f := &explainTrackingFactory{
		ome: omeClient, kube: kubeClient, runtime: runtimeClient, namespace: "prod",
	}
	var out bytes.Buffer
	cmd := newExplainCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"chat", "--ome-namespace", "control-plane", "--output", "json"})

	err := cmd.Execute()

	require.NoError(t, err)
	var got reportv1alpha1.AutoscaleExplainReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &got))
	assert.Equal(t, reportv1alpha1.Metadata{Namespace: "prod", Name: "chat"}, got.Metadata)
	assert.Equal(t, reportv1alpha1.ConfigurationOriginLiveRuntime, got.Content.ActiveConfiguration.Origin)
	assert.Equal(t, "runtime", got.Content.ActiveConfiguration.Runtime.Name)
	assert.Equal(t, []string{"namespace", "ome", "kube", "runtime"}, f.calls)
	require.Len(t, omeBase.Actions(), 1)
	primary, ok := omeBase.Actions()[0].(ktesting.GetAction)
	require.True(t, ok)
	assert.Equal(t, "prod", primary.GetNamespace())
	assert.Equal(t, "chat", primary.GetName())
	require.Len(t, omeClient.gets, 1)
	require.True(t, omeClient.gets[0].hasDeadline)
	assert.WithinDuration(t, omeClient.gets[0].observedAt.Add(10*time.Second), omeClient.gets[0].deadline, 150*time.Millisecond)
	assert.Empty(t, kubeClient.Actions(), "AutoSync must not read or list ControllerRevisions")
	require.Len(t, runtimeReads, 3, "runtime resolution and both inheritance observations are bounded GETs")
	for _, read := range runtimeReads {
		assert.Equal(t, "get", read.verb)
		assert.Equal(t, "*v1beta1.ServingRuntime", read.objectType)
		assert.Equal(t, ctrlclient.ObjectKey{Namespace: "prod", Name: "runtime"}, read.key)
		require.True(t, read.hasDeadline)
		assert.Equal(t, omeClient.gets[0].deadline, read.deadline, "all reads inherit one acquisition deadline")
	}
}

func TestExplainServiceVirtualDeploymentSkipsRuntimeAcquisition(t *testing.T) {
	tests := []struct {
		name string
		spec omev1beta1.InferenceServiceSpec
	}{
		{
			name: "annotation only",
			spec: omev1beta1.InferenceServiceSpec{Engine: &omev1beta1.EngineSpec{}},
		},
		{
			name: "broken runtime reference is irrelevant to the controller early exit",
			spec: omev1beta1.InferenceServiceSpec{
				Runtime: &omev1beta1.ServingRuntimeRef{Name: "missing-runtime"},
				Engine:  &omev1beta1.EngineSpec{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := explainISVCFixture()
			isvc.Spec = tt.spec
			isvc.Annotations = map[string]string{constants.DeploymentMode: string(constants.VirtualDeployment)}
			secondaryClientError := errors.New("secondary clients must not be constructed")
			f := &explainTrackingFactory{
				namespace: "prod", ome: omefake.NewSimpleClientset(isvc),
				kubeErr: secondaryClientError, runtimeErr: secondaryClientError,
			}
			var out bytes.Buffer
			cmd := newExplainCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
			cmd.SetArgs([]string{"chat", "--output", "json"})

			err := cmd.Execute()

			require.NoError(t, err)
			assert.Equal(t, []string{"namespace", "ome"}, f.calls)
			var decoded map[string]any
			require.NoError(t, json.Unmarshal(out.Bytes(), &decoded))
			content := decoded["content"].(map[string]any)
			active := content["activeConfiguration"].(map[string]any)
			assert.Equal(t, "Unavailable", active["state"])
			assert.NotContains(t, active, "runtime")
			components := content["components"].([]any)
			require.Len(t, components, 1)
			component := components[0].(map[string]any)
			assert.Equal(t, "VirtualDeployment", component["deploymentMode"])
			assert.Equal(t, "ServiceAnnotation", component["deploymentModeSource"])
			desired := component["desired"].(map[string]any)
			assert.Equal(t, "Unsupported", desired["state"])
			assert.NotContains(t, desired, "target")
		})
	}
}

func TestExplainFailsClosedWhenRuntimeChangesDuringAcquisition(t *testing.T) {
	const secretResourceVersion = "runtime-resource-version-with-user-secret"
	autoSync := true
	kind := runtimeselector.KindServingRuntime
	isvc := explainISVCFixture()
	isvc.Status.ObservedGeneration = isvc.Generation
	isvc.Spec = omev1beta1.InferenceServiceSpec{
		Runtime: &omev1beta1.ServingRuntimeRef{Name: "runtime", Kind: &kind, AutoSync: &autoSync},
		Engine:  &omev1beta1.EngineSpec{},
	}
	runtimeObject := &omev1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "runtime", Namespace: "prod", UID: types.UID("runtime-uid"),
			ResourceVersion: "29", Generation: 2,
		},
		Spec: omev1beta1.ServingRuntimeSpec{EngineConfig: &omev1beta1.EngineSpec{}},
	}
	base := ctrlfake.NewClientBuilder().WithScheme(explainScheme(t)).WithObjects(runtimeObject).Build()
	getCalls := 0
	runtimeClient := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, client ctrlclient.WithWatch, key ctrlclient.ObjectKey, object ctrlclient.Object, opts ...ctrlclient.GetOption) error {
			getCalls++
			if err := client.Get(ctx, key, object, opts...); err != nil {
				return err
			}
			if getCalls > 1 {
				object.SetResourceVersion(secretResourceVersion)
				object.SetGeneration(3)
			}
			return nil
		},
	})
	var out bytes.Buffer
	cmd := newExplainCmd(factory.Static{
		OME: omefake.NewSimpleClientset(isvc), Kube: k8sfake.NewSimpleClientset(),
		Runtime: runtimeClient, NS: "prod",
	}, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"chat"})

	err := cmd.Execute()

	require.ErrorIs(t, err, effective.ErrRuntimeSnapshotChanged)
	assert.GreaterOrEqual(t, getCalls, 2)
	assert.Empty(t, out.String())
	assert.NotContains(t, err.Error(), secretResourceVersion)
}

func TestExplainFailsClosedWhenRuntimeChangesOnInheritanceObservation(t *testing.T) {
	const secretResourceVersion = "observer-resource-version-with-user-secret"
	autoSync := true
	kind := runtimeselector.KindServingRuntime
	isvc := explainISVCFixture()
	isvc.Status.ObservedGeneration = isvc.Generation
	isvc.Spec = omev1beta1.InferenceServiceSpec{
		Runtime: &omev1beta1.ServingRuntimeRef{Name: "runtime", Kind: &kind, AutoSync: &autoSync},
		Engine:  &omev1beta1.EngineSpec{},
	}
	runtimeObject := &omev1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "runtime", Namespace: "prod", UID: types.UID("runtime-uid"),
			ResourceVersion: "29", Generation: 2,
		},
		Spec: omev1beta1.ServingRuntimeSpec{EngineConfig: &omev1beta1.EngineSpec{}},
	}
	base := ctrlfake.NewClientBuilder().WithScheme(explainScheme(t)).WithObjects(runtimeObject).Build()
	getCalls := 0
	runtimeClient := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, client ctrlclient.WithWatch, key ctrlclient.ObjectKey, object ctrlclient.Object, opts ...ctrlclient.GetOption) error {
			getCalls++
			if err := client.Get(ctx, key, object, opts...); err != nil {
				return err
			}
			if getCalls > 2 {
				object.SetResourceVersion(secretResourceVersion)
				object.SetGeneration(3)
			}
			return nil
		},
	})
	var out bytes.Buffer
	cmd := newExplainCmd(factory.Static{
		OME: omefake.NewSimpleClientset(isvc), Kube: k8sfake.NewSimpleClientset(),
		Runtime: runtimeClient, NS: "prod",
	}, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"chat"})

	err := cmd.Execute()

	require.ErrorIs(t, err, effective.ErrRuntimeSnapshotChanged)
	assert.GreaterOrEqual(t, getCalls, 3)
	assert.Empty(t, out.String())
	assert.NotContains(t, err.Error(), secretResourceVersion)
}

func TestExplainRejectsUnboundLiveRuntimeIdentityWithoutOutput(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(ctrlclient.Object)
	}{
		{name: "missing UID", mutate: func(object ctrlclient.Object) { object.SetUID("") }},
		{name: "missing resourceVersion", mutate: func(object ctrlclient.Object) { object.SetResourceVersion("") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind := runtimeselector.KindServingRuntime
			isvc := explainISVCFixture()
			isvc.Spec = omev1beta1.InferenceServiceSpec{
				Runtime: &omev1beta1.ServingRuntimeRef{Name: "runtime", Kind: &kind},
				Engine:  &omev1beta1.EngineSpec{},
			}
			runtimeObject := &omev1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name: "runtime", Namespace: "prod", UID: "runtime-uid", ResourceVersion: "29",
				},
				Spec: omev1beta1.ServingRuntimeSpec{EngineConfig: &omev1beta1.EngineSpec{}},
			}
			base := ctrlfake.NewClientBuilder().WithScheme(explainScheme(t)).WithObjects(runtimeObject).Build()
			runtimeClient := interceptor.NewClient(base, interceptor.Funcs{
				Get: func(ctx context.Context, client ctrlclient.WithWatch, key ctrlclient.ObjectKey, object ctrlclient.Object, opts ...ctrlclient.GetOption) error {
					if err := client.Get(ctx, key, object, opts...); err != nil {
						return err
					}
					tt.mutate(object)
					return nil
				},
			})
			var out bytes.Buffer
			cmd := newExplainCmd(factory.Static{
				OME: omefake.NewSimpleClientset(isvc), Kube: k8sfake.NewSimpleClientset(),
				Runtime: runtimeClient, NS: "prod",
			}, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
			cmd.SetArgs([]string{"chat"})

			err := cmd.Execute()

			require.ErrorIs(t, err, effective.ErrRuntimeSnapshotUnbindable)
			assert.Empty(t, out.String())
		})
	}
}

func TestExplainRejectsUnboundAutoSelectionModelWithoutOutput(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(ctrlclient.Object)
	}{
		{name: "missing UID", mutate: func(object ctrlclient.Object) { object.SetUID("") }},
		{name: "missing resourceVersion", mutate: func(object ctrlclient.Object) { object.SetResourceVersion("") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			autoSelect := true
			isvc := explainISVCFixture()
			isvc.Spec = omev1beta1.InferenceServiceSpec{
				Model: &omev1beta1.ModelRef{Name: "model"}, Engine: &omev1beta1.EngineSpec{},
			}
			model := &omev1beta1.ClusterBaseModel{
				ObjectMeta: metav1.ObjectMeta{Name: "model", UID: "model-uid", ResourceVersion: "11"},
				Spec:       omev1beta1.BaseModelSpec{ModelFormat: omev1beta1.ModelFormat{Name: "format"}},
			}
			runtimeObject := &omev1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name: "runtime", Namespace: "prod", UID: "runtime-uid", ResourceVersion: "29",
				},
				Spec: omev1beta1.ServingRuntimeSpec{
					EngineConfig: &omev1beta1.EngineSpec{},
					SupportedModelFormats: []omev1beta1.SupportedModelFormat{{
						ModelFormat: &omev1beta1.ModelFormat{Name: "format", Weight: 1}, AutoSelect: &autoSelect,
					}},
				},
			}
			base := ctrlfake.NewClientBuilder().WithScheme(explainScheme(t)).WithObjects(model, runtimeObject).Build()
			runtimeClient := interceptor.NewClient(base, interceptor.Funcs{
				Get: func(ctx context.Context, client ctrlclient.WithWatch, key ctrlclient.ObjectKey, object ctrlclient.Object, opts ...ctrlclient.GetOption) error {
					if err := client.Get(ctx, key, object, opts...); err != nil {
						return err
					}
					if _, ok := object.(*omev1beta1.ClusterBaseModel); ok {
						tt.mutate(object)
					}
					return nil
				},
			})
			var out bytes.Buffer
			cmd := newExplainCmd(factory.Static{
				OME: omefake.NewSimpleClientset(isvc), Kube: k8sfake.NewSimpleClientset(),
				Runtime: runtimeClient, NS: "prod",
			}, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
			cmd.SetArgs([]string{"chat"})

			err := cmd.Execute()

			require.ErrorIs(t, err, effective.ErrRuntimeSnapshotUnbindable)
			assert.Empty(t, out.String())
		})
	}
}

func TestExplainRejectsUnboundActiveRevisionWithoutOutput(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*appsv1.ControllerRevision)
	}{
		{name: "missing UID", mutate: func(revision *appsv1.ControllerRevision) { revision.UID = "" }},
		{name: "missing resourceVersion", mutate: func(revision *appsv1.ControllerRevision) { revision.ResourceVersion = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			autoSync := false
			kind := runtimeselector.KindServingRuntime
			runtimeSpec := &omev1beta1.ServingRuntimeSpec{EngineConfig: &omev1beta1.EngineSpec{}}
			_, shortHash, err := runtimerevision.Hash(runtimeSpec)
			require.NoError(t, err)
			revisionName := runtimerevision.Name(runtimerevision.SourceKind(kind), "prod", "runtime", shortHash)
			isvc := explainISVCFixture()
			isvc.Spec = omev1beta1.InferenceServiceSpec{
				Runtime: &omev1beta1.ServingRuntimeRef{
					Name: "runtime", Kind: &kind, AutoSync: &autoSync, Revision: &revisionName,
				},
				Engine: &omev1beta1.EngineSpec{},
			}
			runtimeObject := &omev1beta1.ServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name: "runtime", Namespace: "prod", UID: "runtime-uid", ResourceVersion: "29",
				},
				Spec: *runtimeSpec.DeepCopy(),
			}
			rawSpec, err := json.Marshal(runtimeSpec)
			require.NoError(t, err)
			revision := &appsv1.ControllerRevision{
				ObjectMeta: metav1.ObjectMeta{
					Name: revisionName, Namespace: "control-plane", UID: "revision-uid", ResourceVersion: "31",
					Labels: map[string]string{
						constants.RuntimeRevisionOfLabelKey:          "runtime",
						constants.RuntimeRevisionOfKindLabelKey:      kind,
						constants.RuntimeRevisionOfNamespaceLabelKey: "prod",
						constants.RuntimeRevisionHashLabelKey:        shortHash,
					},
					Annotations: map[string]string{
						constants.RuntimeRevisionCreatedByKey: constants.RuntimeRevisionCreatedByOMEValue,
					},
				},
				Data: k8sruntime.RawExtension{Raw: rawSpec}, Revision: 1,
			}
			tt.mutate(revision)
			var out bytes.Buffer
			cmd := newExplainCmd(factory.Static{
				OME: omefake.NewSimpleClientset(isvc), Kube: k8sfake.NewSimpleClientset(revision),
				Runtime: ctrlfake.NewClientBuilder().WithScheme(explainScheme(t)).WithObjects(runtimeObject).Build(), NS: "prod",
			}, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
			cmd.SetArgs([]string{"chat", "--ome-namespace", "control-plane"})

			err = cmd.Execute()

			require.ErrorIs(t, err, ErrReturnedRuntimeRevisionIdentityUnbindable)
			assert.Empty(t, out.String())
		})
	}
}

func TestExplainAutoSelectionFailsClosedOnBoundedCandidateTruncation(t *testing.T) {
	isvc := explainISVCFixture()
	isvc.Spec = omev1beta1.InferenceServiceSpec{
		Model:  &omev1beta1.ModelRef{Name: "model"},
		Engine: &omev1beta1.EngineSpec{},
	}
	model := &omev1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model"},
		Spec: omev1beta1.BaseModelSpec{
			ModelFormat: omev1beta1.ModelFormat{Name: "format"},
		},
	}
	base := ctrlfake.NewClientBuilder().WithScheme(explainScheme(t)).WithObjects(model).Build()
	listCalls := 0
	runtimeClient := interceptor.NewClient(base, interceptor.Funcs{
		List: func(ctx context.Context, _ ctrlclient.WithWatch, list ctrlclient.ObjectList, opts ...ctrlclient.ListOption) error {
			listCalls++
			options := &ctrlclient.ListOptions{}
			for _, option := range opts {
				option.ApplyToList(options)
			}
			assert.Equal(t, int64(500), options.Limit)
			assert.Equal(t, "prod", options.Namespace)
			serving, ok := list.(*omev1beta1.ServingRuntimeList)
			require.True(t, ok, "truncation must happen before a cluster-runtime LIST")
			serving.Items = []omev1beta1.ServingRuntime{{ObjectMeta: metav1.ObjectMeta{Name: "secret-candidate", Namespace: "prod"}}}
			serving.Continue = fmt.Sprintf("opaque-secret-token-%d", listCalls)
			return nil
		},
	})
	omeClient := omefake.NewSimpleClientset(isvc)
	kubeClient := k8sfake.NewSimpleClientset()
	var out bytes.Buffer
	cmd := newExplainCmd(factory.Static{
		OME: omeClient, Kube: kubeClient, Runtime: runtimeClient, NS: "prod",
	}, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"chat"})

	err := cmd.Execute()

	require.Error(t, err)
	var truncated *effective.RuntimeSelectionTruncated
	assert.ErrorAs(t, err, &truncated)
	assert.Contains(t, err.Error(), "runtime selection candidates exceeded the CLI collection limit")
	assert.NotContains(t, err.Error(), "secret-candidate")
	assert.NotContains(t, err.Error(), "opaque-secret-token")
	assert.Empty(t, out.String())
	assert.Equal(t, 2, listCalls)
	assert.Empty(t, kubeClient.Actions())
}

func TestExplainCanceledContextStopsBeforeClientConstruction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := &explainTrackingFactory{
		ome:       omefake.NewSimpleClientset(explainISVCFixture()),
		kube:      k8sfake.NewSimpleClientset(),
		runtime:   ctrlfake.NewClientBuilder().WithScheme(explainScheme(t)).Build(),
		namespace: "prod",
	}
	var out bytes.Buffer
	cmd := newExplainCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"chat"})

	err := cmd.Execute()

	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, out.String())
	assert.Equal(t, []string{"namespace"}, f.calls)
}

func TestExplainValidatesBothNamespacesBeforeClientConstruction(t *testing.T) {
	tests := []struct {
		name      string
		factory   factory.Factory
		args      []string
		wantError string
	}{
		{
			name: "namespace resolver", factory: namespaceResultFactory{err: errors.New("namespace backend failed")},
			wantError: "resolve workload namespace: namespace backend failed",
		},
		{name: "empty workload namespace", factory: factory.Static{}, wantError: "workload namespace \"\" is invalid"},
		{
			name: "invalid workload namespace", factory: factory.Static{NS: "Bad_Namespace"},
			wantError: "workload namespace \"Bad_Namespace\" is invalid",
		},
		{
			name: "empty OME namespace", factory: factory.Static{NS: "prod"}, args: []string{"--ome-namespace="},
			wantError: "OME namespace must not be empty",
		},
		{
			name: "invalid OME namespace", factory: factory.Static{NS: "prod"}, args: []string{"--ome-namespace", "Bad_OME"},
			wantError: "OME namespace \"Bad_OME\" is invalid",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			cmd := newExplainCmd(tt.factory, genericiooptions.IOStreams{
				In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{},
			})
			cmd.SetArgs(append([]string{"chat"}, tt.args...))

			err := cmd.Execute()

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantError)
			assert.Empty(t, out.String())
		})
	}
}

func TestExplainRejectsUnboundPrimarySnapshotsBeforeSecondaryClients(t *testing.T) {
	valid := explainISVCFixture
	tests := []struct {
		name string
		got  func() *omev1beta1.InferenceService
		want error
	}{
		{name: "nil", got: func() *omev1beta1.InferenceService { return nil }, want: autoscaleprojection.ErrInferenceServiceRequired},
		{name: "wrong name", got: func() *omev1beta1.InferenceService {
			value := valid()
			value.Name = "hostile-secret-name"
			return value
		}, want: ErrReturnedInferenceServiceNameMismatch},
		{name: "wrong namespace", got: func() *omev1beta1.InferenceService {
			value := valid()
			value.Namespace = "hostile-secret-namespace"
			return value
		}, want: ErrReturnedInferenceServiceNamespaceMismatch},
		{name: "missing UID", got: func() *omev1beta1.InferenceService { value := valid(); value.UID = ""; return value }, want: ErrReturnedInferenceServiceUIDMissing},
		{name: "missing resourceVersion", got: func() *omev1beta1.InferenceService { value := valid(); value.ResourceVersion = ""; return value }, want: ErrReturnedInferenceServiceResourceVersionMissing},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			omeClient := omefake.NewSimpleClientset()
			omeClient.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, k8sruntime.Object, error) {
				return true, tt.got(), nil
			})
			f := &explainTrackingFactory{
				ome: omeClient, kube: k8sfake.NewSimpleClientset(),
				runtime: ctrlfake.NewClientBuilder().WithScheme(explainScheme(t)).Build(), namespace: "prod",
			}
			var out bytes.Buffer
			cmd := newExplainCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
			cmd.SetArgs([]string{"chat"})

			err := cmd.Execute()

			require.ErrorIs(t, err, tt.want)
			assert.NotContains(t, err.Error(), "hostile-secret")
			assert.Equal(t, []string{"namespace", "ome"}, f.calls)
			assert.Empty(t, out.String())
		})
	}
}

func TestExplainRequiresPinnedActiveConfigurationWithoutLiveFallback(t *testing.T) {
	autoSync := false
	kind := runtimeselector.KindServingRuntime
	revision := "missing-secret-revision"
	isvc := explainISVCFixture()
	isvc.Spec = omev1beta1.InferenceServiceSpec{
		Runtime: &omev1beta1.ServingRuntimeRef{
			Name: "runtime", Kind: &kind, AutoSync: &autoSync, Revision: &revision,
		},
		Engine: &omev1beta1.EngineSpec{},
	}
	runtimeObject := &omev1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "runtime", Namespace: "prod", UID: types.UID("runtime-uid"), ResourceVersion: "29",
		},
		Spec: omev1beta1.ServingRuntimeSpec{EngineConfig: &omev1beta1.EngineSpec{}},
	}
	kubeClient := k8sfake.NewSimpleClientset()
	var out bytes.Buffer
	cmd := newExplainCmd(factory.Static{
		OME: omefake.NewSimpleClientset(isvc), Kube: kubeClient,
		Runtime: ctrlfake.NewClientBuilder().WithScheme(explainScheme(t)).WithObjects(runtimeObject).Build(), NS: "prod",
	}, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"chat", "--ome-namespace", "control-plane"})

	err := cmd.Execute()

	require.ErrorIs(t, err, effective.ErrActiveRuntimeUnavailable)
	assert.Contains(t, err.Error(), "require active runtime configuration")
	assert.NotContains(t, err.Error(), revision)
	assert.Empty(t, out.String())
	require.Len(t, kubeClient.Actions(), 1)
	action, ok := kubeClient.Actions()[0].(ktesting.GetAction)
	require.True(t, ok)
	assert.Equal(t, "controllerrevisions", action.GetResource().Resource)
	assert.Equal(t, "control-plane", action.GetNamespace())
	assert.Equal(t, revision, action.GetName())
}

func TestExplainRejectsMismatchedExactRevisionIdentityWithoutOutput(t *testing.T) {
	autoSync := false
	kind := runtimeselector.KindServingRuntime
	revisionName := "expected-revision"
	isvc := explainISVCFixture()
	isvc.Spec = omev1beta1.InferenceServiceSpec{
		Runtime: &omev1beta1.ServingRuntimeRef{
			Name: "runtime", Kind: &kind, AutoSync: &autoSync, Revision: &revisionName,
		},
		Engine: &omev1beta1.EngineSpec{},
	}
	runtimeObject := &omev1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "runtime", Namespace: "prod", ResourceVersion: "29"},
		Spec:       omev1beta1.ServingRuntimeSpec{EngineConfig: &omev1beta1.EngineSpec{}},
	}
	rawSpec, err := json.Marshal(runtimeObject.Spec)
	require.NoError(t, err)
	returnedRevision := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name: "SECRET_RETURNED_REVISION", Namespace: "SECRET_RETURNED_NAMESPACE",
			Labels: map[string]string{
				"ome.io/runtime-revision-of":           "runtime",
				"ome.io/runtime-revision-of-kind":      runtimeselector.KindServingRuntime,
				"ome.io/runtime-revision-of-namespace": "prod",
			},
		},
		Data: k8sruntime.RawExtension{Raw: rawSpec}, Revision: 1,
	}
	kubeClient := k8sfake.NewSimpleClientset()
	kubeClient.PrependReactor("get", "controllerrevisions", func(ktesting.Action) (bool, k8sruntime.Object, error) {
		return true, returnedRevision, nil
	})
	var out bytes.Buffer
	cmd := newExplainCmd(factory.Static{
		OME: omefake.NewSimpleClientset(isvc), Kube: kubeClient,
		Runtime: ctrlfake.NewClientBuilder().WithScheme(explainScheme(t)).WithObjects(runtimeObject).Build(), NS: "prod",
	}, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"chat", "--ome-namespace", "control-plane"})

	err = cmd.Execute()

	require.ErrorIs(t, err, ErrReturnedRuntimeRevisionIdentityMismatch)
	assert.Empty(t, out.String())
	assert.NotContains(t, err.Error(), revisionName)
	assert.NotContains(t, err.Error(), "SECRET_RETURNED_REVISION")
	assert.NotContains(t, err.Error(), "SECRET_RETURNED_NAMESPACE")
}

func TestExplainPreservesPrimaryReadErrorsWithoutSecondaryClientsOrOutput(t *testing.T) {
	groupResource := schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}
	tests := []struct {
		name      string
		cause     error
		assertErr func(*testing.T, error)
	}{
		{
			name: "not found", cause: apierrors.NewNotFound(groupResource, "chat"),
			assertErr: func(t *testing.T, err error) { assert.True(t, apierrors.IsNotFound(err)) },
		},
		{
			name: "forbidden", cause: apierrors.NewForbidden(groupResource, "chat", errors.New("denied")),
			assertErr: func(t *testing.T, err error) { assert.True(t, apierrors.IsForbidden(err)) },
		},
		{
			name: "canceled", cause: context.Canceled,
			assertErr: func(t *testing.T, err error) { assert.ErrorIs(t, err, context.Canceled) },
		},
		{
			name: "deadline", cause: context.DeadlineExceeded,
			assertErr: func(t *testing.T, err error) { assert.ErrorIs(t, err, context.DeadlineExceeded) },
		},
		{
			name: "missing CRD", cause: apierrors.NewGenericServerResponse(404, "get", groupResource, "", "", 0, true),
			assertErr: func(t *testing.T, err error) {
				assert.True(t, apierrors.IsNotFound(err))
				assert.Contains(t, err.Error(), "OME does not appear to be installed")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			omeClient := omefake.NewSimpleClientset()
			omeClient.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, k8sruntime.Object, error) {
				return true, nil, tt.cause
			})
			f := &explainTrackingFactory{
				ome: omeClient, kube: k8sfake.NewSimpleClientset(),
				runtime: ctrlfake.NewClientBuilder().WithScheme(explainScheme(t)).Build(), namespace: "prod",
			}
			var out bytes.Buffer
			cmd := newExplainCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
			cmd.SetArgs([]string{"chat"})

			err := cmd.Execute()

			require.Error(t, err)
			tt.assertErr(t, err)
			assert.Contains(t, err.Error(), `get InferenceService "prod/chat"`)
			assert.Equal(t, []string{"namespace", "ome"}, f.calls)
			assert.Empty(t, out.String())
		})
	}
}

func TestExplainPreservesClientConstructionErrorsAtTheirAcquisitionBoundary(t *testing.T) {
	wantErr := errors.New("client construction failed")
	tests := []struct {
		name      string
		factory   *explainTrackingFactory
		wantCalls []string
		wantText  string
	}{
		{
			name: "OME", factory: &explainTrackingFactory{namespace: "prod", omeErr: wantErr},
			wantCalls: []string{"namespace", "ome"}, wantText: "construct OME client",
		},
		{
			name: "Kubernetes", factory: &explainTrackingFactory{
				namespace: "prod", ome: omefake.NewSimpleClientset(explainISVCFixture()), kubeErr: wantErr,
			},
			wantCalls: []string{"namespace", "ome", "kube"}, wantText: "construct Kubernetes client",
		},
		{
			name: "runtime", factory: &explainTrackingFactory{
				namespace: "prod", ome: omefake.NewSimpleClientset(explainISVCFixture()),
				kube: k8sfake.NewSimpleClientset(), runtimeErr: wantErr,
			},
			wantCalls: []string{"namespace", "ome", "kube", "runtime"}, wantText: "construct runtime client",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			cmd := newExplainCmd(tt.factory, genericiooptions.IOStreams{
				In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{},
			})
			cmd.SetArgs([]string{"chat"})

			err := cmd.Execute()

			require.ErrorIs(t, err, wantErr)
			assert.Contains(t, err.Error(), tt.wantText)
			assert.Equal(t, tt.wantCalls, tt.factory.calls)
			assert.Empty(t, out.String())
		})
	}
}

func TestExplainValidatesArgumentsBeforeFactoryOrCollectorAccess(t *testing.T) {
	deps := explainDependencies{collect: func(
		context.Context, factory.Factory, *namespace.Options, string, paging.Limits,
	) (*explainEvidence, error) {
		panic("collector must not run")
	}}
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing name", want: "accepts 1 arg(s), received 0"},
		{name: "extra name", args: []string{"chat", "other"}, want: "accepts 1 arg(s), received 2"},
		{name: "unsupported output", args: []string{"chat", "--output", "wide"}, want: `unsupported output format "wide"`},
		{name: "invalid name", args: []string{"Bad_Name"}, want: `InferenceService name "Bad_Name" is invalid`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := executeExplain(t, panicFactory{}, deps, tt.args...)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestExplainWritesExactTableAndTreatsDiagnosticStatesAsSuccess(t *testing.T) {
	for _, state := range []reportv1alpha1.AutoscaleExplainState{
		reportv1alpha1.AutoscaleExplainConsistent,
		reportv1alpha1.AutoscaleExplainReportedMismatch,
		reportv1alpha1.AutoscaleExplainInvalid,
	} {
		t.Run(string(state), func(t *testing.T) {
			reportValue := explainCommandReportFixture(t)
			reportValue.Content.Summary.State = state
			deps := explainDependencies{
				collect: func(context.Context, factory.Factory, *namespace.Options, string, paging.Limits) (*explainEvidence, error) {
					return &explainEvidence{inferenceService: explainISVCFixture()}, nil
				},
				project: func(*omev1beta1.InferenceService, *effective.RuntimeState, reportv1alpha1.Clock) (reportv1alpha1.AutoscaleExplainReport, error) {
					return reportValue, nil
				},
			}

			out, err := executeExplain(t, panicFactory{}, deps, "chat")

			require.NoError(t, err)
			if state == reportv1alpha1.AutoscaleExplainConsistent {
				assert.Equal(t,
					"FIELD                ENGINE\n"+
						"STATE                OK\n"+
						"MODE                 Raw\n"+
						"POLICY               Independent\n"+
						"DESIRED              HPA/default\n"+
						"RANGE                1..4\n"+
						"ZERO                 -\n"+
						"EXPECTED-TARGET      Deployment/chat-engine\n"+
						"REPORTED             HPA/default/ome\n"+
						"REPORTED-TARGET      Deployment/chat-engine\n"+
						"CUR/DES              2/2\n"+
						"LAST-SCALE           -\n"+
						"CONDITION-EVIDENCE   reported\n"+
						"CONDITIONS           AbleToScale=True\n"+
						"CHECK                match\n"+
						"WHY                  -\n",
					out,
				)
			} else {
				want := string(state)
				if state == reportv1alpha1.AutoscaleExplainReportedMismatch {
					want = "Mismatch"
				}
				assert.Contains(t, out, want)
			}
		})
	}
}

func TestExplainWritesExactJSONAndYAML(t *testing.T) {
	tests := []struct {
		format string
		want   string
	}{
		{format: "json", want: `{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "AutoscaleExplainReport",
  "metadata": {
    "namespace": "prod",
    "name": "chat"
  },
  "collectedAt": "2026-09-07T19:00:00Z",
  "sources": [
    {
      "kind": "InferenceService",
      "namespace": "prod",
      "name": "chat",
      "uid": "uid-chat",
      "generation": 4,
      "evidence": "Observed",
      "collectedAt": "2026-09-07T19:00:00Z"
    },
    {
      "kind": "ServingRuntime",
      "namespace": "prod",
      "name": "runtime",
      "uid": "runtime-uid",
      "generation": 2,
      "evidence": "Observed",
      "collectedAt": "2026-09-07T19:00:00Z"
    }
  ],
  "content": {
    "summary": {
      "state": "Consistent",
      "statusFreshness": "Current"
    },
    "activeConfiguration": {
      "state": "Available",
      "origin": "LiveRuntime",
      "consistency": "Unknown",
      "runtime": {
        "apiVersion": "ome.io/v1beta1",
        "kind": "ServingRuntime",
        "namespace": "prod",
        "name": "runtime",
        "uid": "runtime-uid",
        "generation": 2
      },
      "inheritance": {
        "state": "Observed",
        "sources": [
          {
            "apiVersion": "ome.io/v1beta1",
            "kind": "ServingRuntime",
            "namespace": "prod",
            "name": "runtime",
            "uid": "runtime-uid",
            "generation": 2
          }
        ]
      }
    },
    "scalingPolicy": {
      "state": "Available",
      "mode": "Independent",
      "source": "default"
    },
    "components": [
      {
        "type": "engine",
        "deploymentMode": "RawDeployment",
        "deploymentModeSource": "Default",
        "desired": {
          "state": "Available",
          "class": "HPA",
          "managedBy": "ome",
          "specSource": "default",
          "target": {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "namespace": "prod",
            "name": "chat-engine"
          },
          "bounds": {
            "state": "Available",
            "minReplicas": 1,
            "maxReplicas": 4
          },
          "scaleToZero": "NotRequested",
          "metricCount": 1,
          "triggerCount": 0
        },
        "reported": {
          "state": "Available",
          "class": "HPA",
          "managedBy": "ome",
          "specSource": "default",
          "targetState": "Reported",
          "target": {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "namespace": "prod",
            "name": "chat-engine"
          },
          "replicas": {
            "state": "Reported",
            "currentReplicas": 2,
            "desiredReplicas": 2
          },
          "conditions": {
            "state": "Reported",
            "items": [
              {
                "type": "AbleToScale",
                "status": "True",
                "lastTransitionTime": "2026-09-07T18:00:00Z"
              }
            ]
          }
        },
        "reconciliation": {
          "state": "Consistent",
          "issues": []
        }
      }
    ],
    "issues": []
  },
  "warnings": []
}
`},
		{format: "yaml", want: `apiVersion: cli.ome.io/v1alpha1
collectedAt: "2026-09-07T19:00:00Z"
content:
  activeConfiguration:
    consistency: Unknown
    inheritance:
      sources:
      - apiVersion: ome.io/v1beta1
        generation: 2
        kind: ServingRuntime
        name: runtime
        namespace: prod
        uid: runtime-uid
      state: Observed
    origin: LiveRuntime
    runtime:
      apiVersion: ome.io/v1beta1
      generation: 2
      kind: ServingRuntime
      name: runtime
      namespace: prod
      uid: runtime-uid
    state: Available
  components:
  - deploymentMode: RawDeployment
    deploymentModeSource: Default
    desired:
      bounds:
        maxReplicas: 4
        minReplicas: 1
        state: Available
      class: HPA
      managedBy: ome
      metricCount: 1
      scaleToZero: NotRequested
      specSource: default
      state: Available
      target:
        apiVersion: apps/v1
        kind: Deployment
        name: chat-engine
        namespace: prod
      triggerCount: 0
    reconciliation:
      issues: []
      state: Consistent
    reported:
      class: HPA
      conditions:
        items:
        - lastTransitionTime: "2026-09-07T18:00:00Z"
          status: "True"
          type: AbleToScale
        state: Reported
      managedBy: ome
      replicas:
        currentReplicas: 2
        desiredReplicas: 2
        state: Reported
      specSource: default
      state: Available
      target:
        apiVersion: apps/v1
        kind: Deployment
        name: chat-engine
        namespace: prod
      targetState: Reported
    type: engine
  issues: []
  scalingPolicy:
    mode: Independent
    source: default
    state: Available
  summary:
    state: Consistent
    statusFreshness: Current
kind: AutoscaleExplainReport
metadata:
  name: chat
  namespace: prod
sources:
- collectedAt: "2026-09-07T19:00:00Z"
  evidence: Observed
  generation: 4
  kind: InferenceService
  name: chat
  namespace: prod
  uid: uid-chat
- collectedAt: "2026-09-07T19:00:00Z"
  evidence: Observed
  generation: 2
  kind: ServingRuntime
  name: runtime
  namespace: prod
  uid: runtime-uid
warnings: []
`},
	}
	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			deps := explainDependencies{
				collect: func(context.Context, factory.Factory, *namespace.Options, string, paging.Limits) (*explainEvidence, error) {
					return &explainEvidence{inferenceService: explainISVCFixture()}, nil
				},
				project: func(*omev1beta1.InferenceService, *effective.RuntimeState, reportv1alpha1.Clock) (reportv1alpha1.AutoscaleExplainReport, error) {
					return explainCommandReportFixture(t), nil
				},
			}

			out, err := executeExplain(t, panicFactory{}, deps, "chat", "--output", tt.format)

			require.NoError(t, err)
			assert.Equal(t, tt.want, out)
		})
	}
}

func TestExplainReturnsAcquisitionProjectionAndWriteErrorsWithoutPartialOutput(t *testing.T) {
	wantAcquire := errors.New("acquisition failed")
	deps := explainDependencies{collect: func(
		context.Context, factory.Factory, *namespace.Options, string, paging.Limits,
	) (*explainEvidence, error) {
		return nil, wantAcquire
	}}
	out, err := executeExplain(t, panicFactory{}, deps, "chat")
	require.ErrorIs(t, err, wantAcquire)
	assert.Empty(t, out)

	wantProject := errors.New("projection failed")
	deps = explainDependencies{
		collect: func(context.Context, factory.Factory, *namespace.Options, string, paging.Limits) (*explainEvidence, error) {
			return &explainEvidence{inferenceService: explainISVCFixture()}, nil
		},
		project: func(*omev1beta1.InferenceService, *effective.RuntimeState, reportv1alpha1.Clock) (reportv1alpha1.AutoscaleExplainReport, error) {
			return reportv1alpha1.AutoscaleExplainReport{}, wantProject
		},
	}
	out, err = executeExplain(t, panicFactory{}, deps, "chat")
	require.ErrorIs(t, err, wantProject)
	assert.Empty(t, out)

	for _, tt := range []struct {
		name   string
		writer io.Writer
		want   error
	}{
		{name: "error", writer: errorWriter{err: io.ErrClosedPipe}, want: io.ErrClosedPipe},
		{name: "short write", writer: shortWriter{}, want: io.ErrShortWrite},
	} {
		for _, format := range []string{"table", "json", "yaml"} {
			t.Run(tt.name+"/"+format, func(t *testing.T) {
				cmd := newExplainCmdWithDependencies(
					panicFactory{},
					genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: tt.writer, ErrOut: &bytes.Buffer{}},
					explainDependencies{
						collect: func(context.Context, factory.Factory, *namespace.Options, string, paging.Limits) (*explainEvidence, error) {
							return &explainEvidence{inferenceService: explainISVCFixture()}, nil
						},
						project: func(*omev1beta1.InferenceService, *effective.RuntimeState, reportv1alpha1.Clock) (reportv1alpha1.AutoscaleExplainReport, error) {
							return explainCommandReportFixture(t), nil
						},
					},
				)
				cmd.SetArgs([]string{"chat", "--output", format})

				err := cmd.Execute()

				require.ErrorIs(t, err, tt.want)
				assert.Contains(t, err.Error(), "write autoscale explain report")
			})
		}
	}
}

func TestExplainMachineMarshalFailureWritesNoBytes(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			reportValue := explainCommandReportFixture(t)
			reportValue.CollectedAt = time.Date(10000, 1, 2, 3, 4, 5, 0, time.UTC)
			deps := explainDependencies{
				collect: func(context.Context, factory.Factory, *namespace.Options, string, paging.Limits) (*explainEvidence, error) {
					return &explainEvidence{inferenceService: explainISVCFixture()}, nil
				},
				project: func(*omev1beta1.InferenceService, *effective.RuntimeState, reportv1alpha1.Clock) (reportv1alpha1.AutoscaleExplainReport, error) {
					return reportValue, nil
				},
			}

			out, err := executeExplain(t, panicFactory{}, deps, "chat", "--output", format)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "marshal report "+format)
			assert.Contains(t, err.Error(), "year outside of range [0,9999]")
			var marshalerError *json.MarshalerError
			assert.ErrorAs(t, err, &marshalerError)
			assert.Empty(t, out)
		})
	}
}

func executeExplain(t *testing.T, f factory.Factory, deps explainDependencies, args ...string) (string, error) {
	t.Helper()
	var output bytes.Buffer
	cmd := newExplainCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &output,
	}, deps)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return output.String(), err
}

func explainISVCFixture() *omev1beta1.InferenceService {
	return &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"), ResourceVersion: "17", Generation: 4,
	}}
}

func explainCommandReportFixture(t *testing.T) reportv1alpha1.AutoscaleExplainReport {
	t.Helper()
	minimum := 1
	kind := runtimeselector.KindServingRuntime
	isvc := explainISVCFixture()
	isvc.Spec = omev1beta1.InferenceServiceSpec{
		Runtime: &omev1beta1.ServingRuntimeRef{Name: "runtime", Kind: &kind},
		Engine: &omev1beta1.EngineSpec{ComponentExtensionSpec: omev1beta1.ComponentExtensionSpec{
			MinReplicas: &minimum, MaxReplicas: 4,
		}},
	}
	isvc.Status.ObservedGeneration = isvc.Generation
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {
			Autoscaler: &omev1beta1.ComponentAutoscalerStatus{
				Class: omev1beta1.AutoscalerHPA, ManagedBy: omev1beta1.AutoscalerManagedByOME,
				SpecSource: "default", CurrentReplicas: 2, DesiredReplicas: 2,
				Conditions: []metav1.Condition{{
					Type: "AbleToScale", Status: metav1.ConditionTrue, Reason: "Observed",
					LastTransitionTime: metav1.NewTime(time.Date(2026, time.September, 7, 18, 0, 0, 0, time.UTC)),
				}},
			},
			ScaleTargetRef: &omev1beta1.ScaleTargetRef{
				APIVersion: "apps/v1", Kind: "Deployment", Name: "chat-engine",
			},
		},
	}
	runtimeObject := &omev1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name: "runtime", Namespace: "prod", UID: types.UID("runtime-uid"),
			Generation: 2, ResourceVersion: "29",
		},
		Spec: omev1beta1.ServingRuntimeSpec{EngineConfig: &omev1beta1.EngineSpec{}},
	}
	runtimeClient := ctrlfake.NewClientBuilder().WithScheme(explainScheme(t)).WithObjects(runtimeObject).Build()
	limits := paging.Limits{PageSize: 500, MaxItems: 1000, MaxPages: 2, RequestTimeout: time.Second}
	live, err := effective.NewBoundedRuntimeResolver(runtimeClient, limits)
	require.NoError(t, err)
	pins, err := effective.NewRuntimePinResolver(k8sfake.NewSimpleClientset().AppsV1(), live, "ome", limits)
	require.NoError(t, err)
	state, err := pins.Resolve(context.Background(), isvc, effective.RuntimeResolveOptions{IncludeHistory: false})
	require.NoError(t, err)

	result, err := autoscaleprojection.ProjectExplain(
		isvc,
		state,
		fixedClock{now: time.Date(2026, time.September, 7, 19, 0, 0, 0, time.UTC)},
	)
	require.NoError(t, err)
	return result
}

type shortWriter struct{}

func (shortWriter) Write([]byte) (int, error) { return 0, nil }

type explainDeadlineObservation struct {
	deadline    time.Time
	hasDeadline bool
	observedAt  time.Time
}

type explainDeadlineOMEClient struct {
	versioned.Interface
	gets []explainDeadlineObservation
}

func (c *explainDeadlineOMEClient) OmeV1beta1() ometypedv1beta1.OmeV1beta1Interface {
	return &explainDeadlineOmeV1beta1{
		OmeV1beta1Interface: c.Interface.OmeV1beta1(), parent: c,
	}
}

type explainDeadlineOmeV1beta1 struct {
	ometypedv1beta1.OmeV1beta1Interface
	parent *explainDeadlineOMEClient
}

func (c *explainDeadlineOmeV1beta1) InferenceServices(namespace string) ometypedv1beta1.InferenceServiceInterface {
	return &explainDeadlineInferenceServices{
		InferenceServiceInterface: c.OmeV1beta1Interface.InferenceServices(namespace), parent: c.parent,
	}
}

type explainDeadlineInferenceServices struct {
	ometypedv1beta1.InferenceServiceInterface
	parent *explainDeadlineOMEClient
}

func (c *explainDeadlineInferenceServices) Get(
	ctx context.Context,
	name string,
	options metav1.GetOptions,
) (*omev1beta1.InferenceService, error) {
	observedAt := time.Now()
	deadline, hasDeadline := ctx.Deadline()
	c.parent.gets = append(c.parent.gets, explainDeadlineObservation{
		deadline: deadline, hasDeadline: hasDeadline, observedAt: observedAt,
	})
	return c.InferenceServiceInterface.Get(ctx, name, options)
}

type explainRuntimeRead struct {
	verb        string
	objectType  string
	key         ctrlclient.ObjectKey
	deadline    time.Time
	hasDeadline bool
}

type explainTrackingFactory struct {
	ome          versioned.Interface
	kube         kubernetes.Interface
	runtime      ctrlclient.Client
	namespace    string
	namespaceErr error
	omeErr       error
	kubeErr      error
	runtimeErr   error
	calls        []string
}

func (*explainTrackingFactory) RESTConfig() (*rest.Config, error) {
	panic("unexpected RESTConfig call")
}

func (f *explainTrackingFactory) Namespace() (string, bool, error) {
	f.calls = append(f.calls, "namespace")
	return f.namespace, false, f.namespaceErr
}

func (f *explainTrackingFactory) OMEClient() (versioned.Interface, error) {
	f.calls = append(f.calls, "ome")
	return f.ome, f.omeErr
}

func (f *explainTrackingFactory) KubeClient() (kubernetes.Interface, error) {
	f.calls = append(f.calls, "kube")
	return f.kube, f.kubeErr
}

func (f *explainTrackingFactory) RuntimeClient() (ctrlclient.Client, error) {
	f.calls = append(f.calls, "runtime")
	return f.runtime, f.runtimeErr
}

func explainScheme(t *testing.T) *k8sruntime.Scheme {
	t.Helper()
	scheme := k8sruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(omev1beta1.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
	return scheme
}
