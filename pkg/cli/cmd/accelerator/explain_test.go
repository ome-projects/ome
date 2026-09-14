package accelerator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/acceleratorprojection"
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

func TestExplainProductionAcquisitionUsesOnlyBoundedExactReads(t *testing.T) {
	isvc, runtimeObject, class := acceleratorCommandFixtures()
	omeClient := omefake.NewSimpleClientset(isvc, class)
	runtimeClient := ctrlfake.NewClientBuilder().WithScheme(acceleratorScheme(t)).WithObjects(runtimeObject).Build()
	kubeClient := k8sfake.NewSimpleClientset()
	f := factory.Static{OME: omeClient, Kube: kubeClient, Runtime: runtimeClient, NS: "prod"}
	var output bytes.Buffer
	cmd := newExplainCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{},
	}, explainDependencies{
		clock: fixedCommandClock(), limits: commandLimits(),
		collect: collectAcceleratorEvidence, project: acceleratorprojection.Project,
	})
	cmd.SetArgs([]string{"chat", "--ome-namespace", "control-plane", "-o", "json"})

	err := cmd.Execute()

	require.NoError(t, err)
	var got reportv1alpha1.AcceleratorExplainReport
	require.NoError(t, json.Unmarshal(output.Bytes(), &got))
	assert.Equal(t, reportv1alpha1.AcceleratorExplainReported, got.Content.Summary.State)
	assert.Equal(t, "gpu-a", got.Content.Components[0].Selection.Class)
	require.Len(t, omeClient.Actions(), 2)
	for _, action := range omeClient.Actions() {
		assert.Equal(t, "get", action.GetVerb())
		assert.False(t, action.Matches("list", "acceleratorclasses"))
		assert.False(t, action.Matches("create", "*"))
		assert.False(t, action.Matches("update", "*"))
		assert.False(t, action.Matches("patch", "*"))
		assert.False(t, action.Matches("delete", "*"))
	}
	assert.Equal(t, "inferenceservices", omeClient.Actions()[0].GetResource().Resource)
	assert.Equal(t, "acceleratorclasses", omeClient.Actions()[1].GetResource().Resource)
	classGet := omeClient.Actions()[1].(ktesting.GetAction)
	assert.Empty(t, classGet.GetNamespace())
	assert.Equal(t, "gpu-a", classGet.GetName())
	assert.Empty(t, kubeClient.Actions(), "AutoSync must not read revisions")
}

func TestExplainProductionAcquisitionUsesOneDeadlineForEveryJoin(t *testing.T) {
	isvc, runtimeObject, class := acceleratorCommandFixtures()
	omeBase := omefake.NewSimpleClientset(isvc, class)
	omeClient := &acceleratorDeadlineOMEClient{Interface: omeBase}
	runtimeBase := ctrlfake.NewClientBuilder().WithScheme(acceleratorScheme(t)).
		WithObjects(runtimeObject).Build()
	var runtimeReads []acceleratorDeadlineObservation
	runtimeClient := interceptor.NewClient(runtimeBase, interceptor.Funcs{
		Get: func(ctx context.Context, client ctrlclient.WithWatch, key ctrlclient.ObjectKey,
			object ctrlclient.Object, opts ...ctrlclient.GetOption,
		) error {
			deadline, ok := ctx.Deadline()
			runtimeReads = append(runtimeReads, acceleratorDeadlineObservation{
				kind: fmt.Sprintf("get:%T", object), deadline: deadline, hasDeadline: ok,
			})
			return client.Get(ctx, key, object, opts...)
		},
		List: func(ctx context.Context, client ctrlclient.WithWatch,
			list ctrlclient.ObjectList, opts ...ctrlclient.ListOption,
		) error {
			deadline, ok := ctx.Deadline()
			runtimeReads = append(runtimeReads, acceleratorDeadlineObservation{
				kind: fmt.Sprintf("list:%T", list), deadline: deadline, hasDeadline: ok,
			})
			return client.List(ctx, list, opts...)
		},
	})
	limits := commandLimits()
	limits.RequestTimeout = 2 * time.Second
	f := &acceleratorTrackingFactory{
		ome: omeClient, kube: k8sfake.NewSimpleClientset(), runtime: runtimeClient,
		namespace: "prod",
	}
	var output bytes.Buffer
	cmd := newExplainCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{},
	}, explainDependencies{
		clock: fixedCommandClock(), limits: limits,
		collect: collectAcceleratorEvidence, project: acceleratorprojection.Project,
	})
	cmd.SetArgs([]string{"chat", "-o", "json"})
	started := time.Now()

	err := cmd.Execute()

	require.NoError(t, err)
	require.Len(t, omeClient.reads, 2)
	require.NotEmpty(t, runtimeReads)
	wantDeadline := omeClient.reads[0].deadline
	assert.True(t, omeClient.reads[0].hasDeadline)
	assert.WithinDuration(t, started.Add(limits.RequestTimeout), wantDeadline, 250*time.Millisecond)
	for _, read := range append(append([]acceleratorDeadlineObservation{}, omeClient.reads...), runtimeReads...) {
		assert.True(t, read.hasDeadline, read.kind)
		assert.True(t, read.deadline.Equal(wantDeadline), "%s got %s, want %s",
			read.kind, read.deadline, wantDeadline)
	}
	assert.Equal(t, []string{"InferenceService", "AcceleratorClass"}, []string{
		omeClient.reads[0].kind, omeClient.reads[1].kind,
	})
}

func TestExplainReadsAtMostTwoDistinctReportedClassesInStableOrder(t *testing.T) {
	isvc, runtimeObject, classA := acceleratorCommandFixtures()
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{Runner: &omev1beta1.RunnerSpec{}}
	runtimeObject.Spec.DecoderConfig = &omev1beta1.DecoderSpec{}
	isvc.Status.Components[omev1beta1.DecoderComponent] = omev1beta1.ComponentStatusSpec{
		SelectedAccelerator: &omev1beta1.AcceleratorSelection{AcceleratorClass: "gpu-b"},
	}
	classB := classA.DeepCopy()
	classB.Name = "gpu-b"
	classB.UID = "class-b-uid"
	classB.ResourceVersion = "10"
	omeClient := omefake.NewSimpleClientset(isvc, classA, classB)
	f := factory.Static{
		OME: omeClient, Kube: k8sfake.NewSimpleClientset(),
		Runtime: ctrlfake.NewClientBuilder().WithScheme(acceleratorScheme(t)).
			WithObjects(runtimeObject).Build(),
		NS: "prod",
	}
	var output bytes.Buffer
	cmd := newExplainCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{},
	}, fixedExplainDependencies())
	cmd.SetArgs([]string{"chat", "-o", "json"})

	err := cmd.Execute()

	require.NoError(t, err)
	var classGets []string
	for _, action := range omeClient.Actions() {
		if action.Matches("get", "acceleratorclasses") {
			classGets = append(classGets, action.(ktesting.GetAction).GetName())
		}
	}
	assert.Equal(t, []string{"gpu-a", "gpu-b"}, classGets)
	var got reportv1alpha1.AcceleratorExplainReport
	require.NoError(t, json.Unmarshal(output.Bytes(), &got))
	require.Len(t, got.Content.Components, 2)
	assert.Equal(t, reportv1alpha1.RuntimeComponentEngine, got.Content.Components[0].Type)
	assert.Equal(t, reportv1alpha1.RuntimeComponentDecoder, got.Content.Components[1].Type)
}

func TestExplainUsesBoundControllerRevisionForPinAwareBaseRequests(t *testing.T) {
	isvc, runtimeObject, class := acceleratorCommandFixtures()
	autoSync := false
	isvc.Spec.Runtime.AutoSync = &autoSync
	pinnedSpec := runtimeObject.Spec.DeepCopy()
	pinnedSpec.EngineConfig.Runner = &omev1beta1.RunnerSpec{Container: corev1.Container{
		Name: "runner", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("7"),
		}},
	}}
	fullHash, shortHash, err := runtimerevision.Hash(pinnedSpec)
	require.NoError(t, err)
	require.Len(t, fullHash, 64)
	revisionName := runtimerevision.Name(
		runtimerevision.SourceKind(runtimeselector.KindServingRuntime),
		"prod", "runtime", shortHash,
	)
	isvc.Status.PinnedRevisionName = revisionName
	raw, err := json.Marshal(pinnedSpec)
	require.NoError(t, err)
	revision := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name: revisionName, Namespace: "control-plane", UID: "revision-uid",
			ResourceVersion: "31",
			Labels: map[string]string{
				constants.RuntimeRevisionOfLabelKey:          "runtime",
				constants.RuntimeRevisionOfKindLabelKey:      runtimeselector.KindServingRuntime,
				constants.RuntimeRevisionOfNamespaceLabelKey: "prod",
				constants.RuntimeRevisionHashLabelKey:        shortHash,
			},
			Annotations: map[string]string{
				constants.RuntimeRevisionCreatedByKey: constants.RuntimeRevisionCreatedByOMEValue,
			},
		},
		Data: runtime.RawExtension{Raw: raw}, Revision: 1,
	}
	f := factory.Static{
		OME:  omefake.NewSimpleClientset(isvc, class),
		Kube: k8sfake.NewSimpleClientset(revision),
		Runtime: ctrlfake.NewClientBuilder().WithScheme(acceleratorScheme(t)).
			WithObjects(runtimeObject).Build(),
		NS: "prod",
	}
	var output bytes.Buffer
	cmd := newExplainCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{},
	}, fixedExplainDependencies())
	cmd.SetArgs([]string{"chat", "--ome-namespace", "control-plane", "-o", "json"})

	err = cmd.Execute()

	require.NoError(t, err)
	var got reportv1alpha1.AcceleratorExplainReport
	require.NoError(t, json.Unmarshal(output.Bytes(), &got))
	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, []reportv1alpha1.AcceleratorResourceRequest{{
		Name: "cpu", Quantity: "7",
	}}, got.Content.Components[0].Requests.Base)
	assert.Contains(t, got.Sources, reportv1alpha1.AcceleratorSourceReference{
		Kind: "ControllerRevision", Namespace: "control-plane", Name: revisionName,
		Evidence:    reportv1alpha1.EvidenceObserved,
		CollectedAt: fixedCommandClock().Now(),
	})
}

func TestExplainNeverEmitsArbitraryServiceRuntimeOrClassPayloads(t *testing.T) {
	const secret = "ghp_arbitrary-payload-secret"
	isvc, runtimeObject, class := acceleratorCommandFixtures()
	isvc.Annotations = map[string]string{"private.example/token": secret}
	isvc.Spec.Engine.Runner.Container.Env = []corev1.EnvVar{{Name: "TOKEN", Value: secret}}
	isvc.Status.Components[omev1beta1.EngineComponent].SelectedAccelerator.Reason = secret
	isvc.Status.Components[omev1beta1.EngineComponent].SelectedAccelerator.NodeSelector =
		map[string]string{"private.example/token": "secret-value"}
	runtimeObject.Annotations = map[string]string{"private.example/token": secret}
	runtimeObject.Spec.EngineConfig.Runner = &omev1beta1.RunnerSpec{Container: corev1.Container{
		Name: "runner", Args: []string{secret},
		Env: []corev1.EnvVar{{Name: "TOKEN", Value: secret}},
	}}
	class.Annotations = map[string]string{"private.example/token": secret}
	class.Status.Nodes = []string{secret}
	f := factory.Static{
		OME: omefake.NewSimpleClientset(isvc, class), Kube: k8sfake.NewSimpleClientset(),
		Runtime: ctrlfake.NewClientBuilder().WithScheme(acceleratorScheme(t)).
			WithObjects(runtimeObject).Build(),
		NS: "prod",
	}
	var output bytes.Buffer
	cmd := newExplainCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{},
	}, fixedExplainDependencies())
	cmd.SetArgs([]string{"chat", "-o", "json"})

	err := cmd.Execute()

	require.NoError(t, err)
	assert.NotContains(t, output.String(), secret)
	assert.NotContains(t, output.String(), "private.example/token")
	assert.NotContains(t, output.String(), "resourceVersion")
	assert.NotContains(t, strings.ToLower(output.String()), "uid")
	assert.Contains(t, output.String(), `"digest": "rs1:`)
}

func TestExplainProductionProjectionPreservesBaseRequestsWithoutSelector(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService, *omev1beta1.ServingRuntime)
		want   []reportv1alpha1.AcceleratorResourceRequest
	}{
		{
			name: "direct service request",
			mutate: func(isvc *omev1beta1.InferenceService, _ *omev1beta1.ServingRuntime) {
				isvc.Spec.Engine.Runner.Container.Resources.Requests = corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("3"),
				}
			},
			want: []reportv1alpha1.AcceleratorResourceRequest{{Name: "cpu", Quantity: "3"}},
		},
		{
			name: "inherited runtime request",
			mutate: func(_ *omev1beta1.InferenceService, runtimeObject *omev1beta1.ServingRuntime) {
				runtimeObject.Spec.EngineConfig.Runner = &omev1beta1.RunnerSpec{
					Container: corev1.Container{Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceName("example.com/gpu"): resource.MustParse("2"),
						},
					}},
				}
			},
			want: []reportv1alpha1.AcceleratorResourceRequest{{
				Name: "example.com/gpu", Quantity: "2",
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc, runtimeObject, _ := acceleratorCommandFixtures()
			isvc.Spec.AcceleratorSelector = nil
			isvc.Status.Components = nil
			runtimeObject.Spec.AcceleratorRequirements = nil
			tt.mutate(isvc, runtimeObject)
			f := factory.Static{
				OME: omefake.NewSimpleClientset(isvc), Kube: k8sfake.NewSimpleClientset(),
				Runtime: ctrlfake.NewClientBuilder().WithScheme(acceleratorScheme(t)).
					WithObjects(runtimeObject).Build(),
				NS: "prod",
			}
			var output bytes.Buffer
			cmd := newExplainCmdWithDependencies(f, genericiooptions.IOStreams{
				In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{},
			}, fixedExplainDependencies())
			cmd.SetArgs([]string{"chat", "-o", "json"})

			require.NoError(t, cmd.Execute())
			var got reportv1alpha1.AcceleratorExplainReport
			require.NoError(t, json.Unmarshal(output.Bytes(), &got))
			require.Len(t, got.Content.Components, 1)
			component := got.Content.Components[0]
			assert.Equal(t, reportv1alpha1.AcceleratorSelectionNotConfigured,
				component.Selection.State)
			assert.Equal(t, reportv1alpha1.AcceleratorBaseRequestsAvailable,
				component.Requests.BaseState)
			assert.Equal(t, reportv1alpha1.AcceleratorRequestsNotConfigured,
				component.Requests.EffectiveState)
			assert.Equal(t, tt.want, component.Requests.Base)
		})
	}
}

func TestExplainProductionWideShowsAbsentRequestEvidenceAndProvenance(t *testing.T) {
	isvc, runtimeObject, class := acceleratorCommandFixtures()
	isvc.Status.Components[omev1beta1.EngineComponent].SelectedAccelerator.ResourceRequests = nil
	f := factory.Static{
		OME: omefake.NewSimpleClientset(isvc, class), Kube: k8sfake.NewSimpleClientset(),
		Runtime: ctrlfake.NewClientBuilder().WithScheme(acceleratorScheme(t)).
			WithObjects(runtimeObject).Build(),
		NS: "prod",
	}
	var output bytes.Buffer
	cmd := newExplainCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{},
	}, fixedExplainDependencies())
	cmd.SetArgs([]string{"chat", "-o", "wide"})

	require.NoError(t, cmd.Execute())
	assert.Contains(t, output.String(), "STATUS_FRESHNESS")
	assert.Contains(t, output.String(), "REASON_STATE")
	assert.Contains(t, output.String(), "BASE_STATE")
	assert.Contains(t, output.String(), "EFFECTIVE_STATE")
	assert.Contains(t, output.String(), "NotReported")
	assert.Contains(t, output.String(), "RequestsNotReported")
	assert.Contains(t, output.String(), "SOURCE")
	assert.Contains(t, output.String(), "InferenceService")
	assert.Contains(t, output.String(), "WARNING")
	assert.NotContains(t, output.String(), "UID")
	assert.NotContains(t, output.String(), "example.com/gpu=1")
}

func TestExplainStaleStatusSkipsAcceleratorClassRead(t *testing.T) {
	isvc, runtimeObject, class := acceleratorCommandFixtures()
	isvc.Status.ObservedGeneration--
	omeClient := omefake.NewSimpleClientset(isvc, class)
	f := factory.Static{
		OME: omeClient, Kube: k8sfake.NewSimpleClientset(),
		Runtime: ctrlfake.NewClientBuilder().WithScheme(acceleratorScheme(t)).WithObjects(runtimeObject).Build(),
		NS:      "prod",
	}
	var output bytes.Buffer
	cmd := newExplainCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{},
	}, fixedExplainDependencies())
	cmd.SetArgs([]string{"chat", "-o", "json"})

	err := cmd.Execute()

	require.NoError(t, err)
	require.Len(t, omeClient.Actions(), 1)
	assert.Equal(t, "inferenceservices", omeClient.Actions()[0].GetResource().Resource)
	var got reportv1alpha1.AcceleratorExplainReport
	require.NoError(t, json.Unmarshal(output.Bytes(), &got))
	assert.Equal(t, reportv1alpha1.AcceleratorSelectionUnavailable, got.Content.Components[0].Selection.State)
	assert.Empty(t, got.Content.Components[0].Selection.Class)
}

func TestExplainVirtualDeploymentSkipsRuntimeAndKubernetesClients(t *testing.T) {
	isvc, _, _ := acceleratorCommandFixtures()
	isvc.Spec.Runtime = nil
	isvc.Annotations = map[string]string{
		constants.DeploymentMode: string(constants.VirtualDeployment),
	}
	isvc.Status.Components = nil
	omeClient := omefake.NewSimpleClientset(isvc)
	f := factory.Static{OME: omeClient, NS: "prod"}
	var output bytes.Buffer
	cmd := newExplainCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{},
	}, fixedExplainDependencies())
	cmd.SetArgs([]string{"chat", "-o", "json"})

	err := cmd.Execute()

	require.NoError(t, err)
	require.Len(t, omeClient.Actions(), 1)
	assert.Equal(t, "inferenceservices", omeClient.Actions()[0].GetResource().Resource)
	var got reportv1alpha1.AcceleratorExplainReport
	require.NoError(t, json.Unmarshal(output.Bytes(), &got))
	assert.Equal(t, reportv1alpha1.AcceleratorExplainPartial, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.AcceleratorSelectionUnavailable, got.Content.Components[0].Selection.State)
	assert.Contains(t, got.Content.Components[0].Issues,
		reportv1alpha1.AcceleratorIssueActiveConfigurationUnavailable)
}

func TestExplainMissingRuntimeRemainsUnavailableInsteadOfFailing(t *testing.T) {
	isvc, _, _ := acceleratorCommandFixtures()
	isvc.Spec.Runtime = nil
	isvc.Spec.AcceleratorSelector = nil
	isvc.Status.Components = nil
	omeClient := omefake.NewSimpleClientset(isvc)
	f := factory.Static{
		OME: omeClient, Kube: k8sfake.NewSimpleClientset(),
		Runtime: ctrlfake.NewClientBuilder().WithScheme(acceleratorScheme(t)).Build(),
		NS:      "prod",
	}
	var output bytes.Buffer
	cmd := newExplainCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{},
	}, fixedExplainDependencies())
	cmd.SetArgs([]string{"chat", "-o", "json"})

	err := cmd.Execute()

	require.NoError(t, err)
	var got reportv1alpha1.AcceleratorExplainReport
	require.NoError(t, json.Unmarshal(output.Bytes(), &got))
	assert.Equal(t, reportv1alpha1.AcceleratorExplainPartial, got.Content.Summary.State)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AcceleratorBaseRequestsUnavailable,
		component.Requests.BaseState)
	assert.Equal(t, reportv1alpha1.AcceleratorRequestsNotConfigured,
		component.Requests.EffectiveState)
	assert.Contains(t, component.Issues,
		reportv1alpha1.AcceleratorIssueActiveConfigurationUnavailable)
}

func TestExplainUnverifiableRuntimeSkipsReportedClassJoin(t *testing.T) {
	isvc, _, class := acceleratorCommandFixtures()
	isvc.Spec.Runtime = nil
	omeClient := omefake.NewSimpleClientset(isvc, class)
	f := factory.Static{
		OME: omeClient, Kube: k8sfake.NewSimpleClientset(),
		Runtime: ctrlfake.NewClientBuilder().WithScheme(acceleratorScheme(t)).Build(),
		NS:      "prod",
	}
	var output bytes.Buffer
	cmd := newExplainCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{},
	}, fixedExplainDependencies())
	cmd.SetArgs([]string{"chat", "-o", "json"})

	err := cmd.Execute()

	require.NoError(t, err)
	require.Len(t, omeClient.Actions(), 1)
	assert.Equal(t, "inferenceservices", omeClient.Actions()[0].GetResource().Resource)
	var got reportv1alpha1.AcceleratorExplainReport
	require.NoError(t, json.Unmarshal(output.Bytes(), &got))
	assert.Equal(t, reportv1alpha1.AcceleratorSelectionUnavailable,
		got.Content.Components[0].Selection.State)
	assert.Equal(t, reportv1alpha1.AcceleratorBaseRequestsUnavailable,
		got.Content.Components[0].Requests.BaseState)
	assert.Empty(t, got.Content.Components[0].Selection.Class)
}

func TestExplainClassReadFailuresAreTypedAndDoNotEchoServerMessages(t *testing.T) {
	const secret = "ghp_server-error-secret"
	tests := []struct {
		name string
		err  error
		want reportv1alpha1.AcceleratorClassState
	}{
		{name: "object not found despite hostile unsupported phrase", err: &apierrors.StatusError{
			ErrStatus: metav1.Status{
				Reason:  metav1.StatusReasonNotFound,
				Message: "the server could not find the requested resource: " + secret,
				Details: &metav1.StatusDetails{
					Name: "gpu-a", Group: "ome.io", Kind: "acceleratorclasses",
				},
			},
		}, want: reportv1alpha1.AcceleratorClassNotFound},
		{name: "unsupported API", err: apierrors.NewGenericServerResponse(
			404, "get",
			schema.GroupResource{Group: "ome.io", Resource: "acceleratorclasses"},
			"", secret, 0, true,
		), want: reportv1alpha1.AcceleratorClassUnsupportedAPI},
		{name: "forbidden", err: apierrors.NewForbidden(schema.GroupResource{Group: "ome.io", Resource: "acceleratorclasses"}, "gpu-a", errors.New(secret)), want: reportv1alpha1.AcceleratorClassForbidden},
		{name: "unreadable", err: errors.New(secret), want: reportv1alpha1.AcceleratorClassUnreadable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc, runtimeObject, _ := acceleratorCommandFixtures()
			omeClient := omefake.NewSimpleClientset(isvc)
			omeClient.PrependReactor("get", "acceleratorclasses", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, tt.err
			})
			f := factory.Static{
				OME: omeClient, Kube: k8sfake.NewSimpleClientset(),
				Runtime: ctrlfake.NewClientBuilder().WithScheme(acceleratorScheme(t)).WithObjects(runtimeObject).Build(),
				NS:      "prod",
			}
			var output bytes.Buffer
			cmd := newExplainCmdWithDependencies(f, genericiooptions.IOStreams{
				In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{},
			}, fixedExplainDependencies())
			cmd.SetArgs([]string{"chat", "-o", "json"})

			err := cmd.Execute()

			require.NoError(t, err)
			var got reportv1alpha1.AcceleratorExplainReport
			require.NoError(t, json.Unmarshal(output.Bytes(), &got))
			assert.Equal(t, tt.want, got.Content.Components[0].Class.State)
			assert.NotContains(t, output.String(), secret)
		})
	}
}

func TestExplainWrappedClassReadFailuresRemainTypedAndRedacted(t *testing.T) {
	const (
		wrapperSecret = "ghp_hostile-wrapper-secret"
		causeSecret   = "ghp_hostile-cause-secret"
		missingPhrase = "the server could not find the requested resource"
	)
	tests := []struct {
		name string
		err  error
		want reportv1alpha1.AcceleratorClassState
	}{
		{
			name: "wrapped named object not found",
			err: fmt.Errorf("%s: %w", wrapperSecret, &apierrors.StatusError{
				ErrStatus: metav1.Status{
					Reason:  metav1.StatusReasonNotFound,
					Message: missingPhrase + ": " + causeSecret,
					Details: &metav1.StatusDetails{
						Name: "gpu-a", Group: "ome.io", Kind: "acceleratorclasses",
					},
				},
			}),
			want: reportv1alpha1.AcceleratorClassNotFound,
		},
		{
			name: "wrapped resource not found",
			err: fmt.Errorf("%s: %w", wrapperSecret,
				apierrors.NewGenericServerResponse(
					404, "get",
					schema.GroupResource{Group: "ome.io", Resource: "acceleratorclasses"},
					"", causeSecret, 0, true,
				),
			),
			want: reportv1alpha1.AcceleratorClassUnsupportedAPI,
		},
		{
			name: "plain hostile phrase",
			err:  errors.New(missingPhrase + ": " + causeSecret),
			want: reportv1alpha1.AcceleratorClassUnreadable,
		},
	}
	for _, test := range tests {
		for _, format := range []string{"table", "wide", "json", "yaml"} {
			t.Run(test.name+"/"+format, func(t *testing.T) {
				isvc, runtimeObject, _ := acceleratorCommandFixtures()
				omeClient := omefake.NewSimpleClientset(isvc)
				omeClient.PrependReactor(
					"get", "acceleratorclasses",
					func(ktesting.Action) (bool, runtime.Object, error) {
						return true, nil, test.err
					},
				)
				f := factory.Static{
					OME: omeClient, Kube: k8sfake.NewSimpleClientset(),
					Runtime: ctrlfake.NewClientBuilder().WithScheme(acceleratorScheme(t)).
						WithObjects(runtimeObject).Build(),
					NS: "prod",
				}
				var captured reportv1alpha1.AcceleratorExplainReport
				dependencies := fixedExplainDependencies()
				dependencies.project = func(
					isvc *omev1beta1.InferenceService,
					base effective.AcceleratorBaseResolution,
					classes map[string]acceleratorprojection.AcceleratorClassEvidence,
					clock reportv1alpha1.Clock,
				) (reportv1alpha1.AcceleratorExplainReport, error) {
					projected, err := acceleratorprojection.Project(isvc, base, classes, clock)
					captured = projected
					return projected, err
				}
				var output bytes.Buffer
				cmd := newExplainCmdWithDependencies(f, genericiooptions.IOStreams{
					In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{},
				}, dependencies)
				cmd.SetArgs([]string{"chat", "-o", format})

				require.NoError(t, cmd.Execute())
				require.Len(t, captured.Content.Components, 1)
				assert.Equal(t, test.want, captured.Content.Components[0].Class.State)
				assert.NotEmpty(t, output.String())
				assert.NotContains(t, output.String(), wrapperSecret)
				assert.NotContains(t, output.String(), causeSecret)
				assert.NotContains(t, output.String(), missingPhrase)
			})
		}
	}
}

func TestExplainPrimaryReadFailuresUseFixedSafeClassification(t *testing.T) {
	const secret = "ghp_hostile-proxy-error-secret"
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "not found", err: apierrors.NewNotFound(
			schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}, "chat",
		), want: `read InferenceService "prod/chat": not found`},
		{name: "unsupported API", err: &apierrors.StatusError{ErrStatus: metav1.Status{
			Reason:  metav1.StatusReasonNotFound,
			Message: "the server could not find the requested resource: " + secret,
		}}, want: `read InferenceService "prod/chat": unsupported API`},
		{name: "forbidden", err: apierrors.NewForbidden(
			schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"},
			"chat", errors.New(secret),
		), want: `read InferenceService "prod/chat": forbidden`},
		{name: "unauthorized", err: apierrors.NewUnauthorized(secret),
			want: `read InferenceService "prod/chat": forbidden`},
		{name: "timeout", err: apierrors.NewTimeoutError(secret, 1),
			want: `read InferenceService "prod/chat": timed out`},
		{name: "unreadable", err: errors.New(secret),
			want: `read InferenceService "prod/chat": unreadable`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			omeClient := omefake.NewSimpleClientset()
			omeClient.PrependReactor(
				"get", "inferenceservices",
				func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, tt.err
				},
			)
			cmd := newExplainCmdWithDependencies(
				factory.Static{OME: omeClient, NS: "prod"},
				genericiooptions.IOStreams{
					In: &bytes.Buffer{}, Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{},
				}, fixedExplainDependencies(),
			)
			cmd.SetArgs([]string{"chat"})

			err := cmd.Execute()

			require.Error(t, err)
			assert.Equal(t, tt.want, err.Error())
			assert.NotContains(t, err.Error(), secret)
			assert.NotContains(t, fmt.Sprintf("%#v", err), secret)
			assert.ErrorIs(t, err, tt.err)
		})
	}
}

func TestExplainMismatchedOrUnboundClassResponseBecomesInvalidEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.AcceleratorClass)
	}{
		{name: "wrong name", mutate: func(class *omev1beta1.AcceleratorClass) { class.Name = "gpu-b" }},
		{name: "namespace set", mutate: func(class *omev1beta1.AcceleratorClass) { class.Namespace = "prod" }},
		{name: "uid missing", mutate: func(class *omev1beta1.AcceleratorClass) { class.UID = "" }},
		{name: "resource version missing", mutate: func(class *omev1beta1.AcceleratorClass) { class.ResourceVersion = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc, runtimeObject, class := acceleratorCommandFixtures()
			tt.mutate(class)
			omeClient := omefake.NewSimpleClientset(isvc)
			omeClient.PrependReactor("get", "acceleratorclasses", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, class.DeepCopy(), nil
			})
			f := factory.Static{
				OME: omeClient, Kube: k8sfake.NewSimpleClientset(),
				Runtime: ctrlfake.NewClientBuilder().WithScheme(acceleratorScheme(t)).WithObjects(runtimeObject).Build(),
				NS:      "prod",
			}
			var output bytes.Buffer
			cmd := newExplainCmdWithDependencies(f, genericiooptions.IOStreams{
				In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{},
			}, fixedExplainDependencies())
			cmd.SetArgs([]string{"chat", "-o", "json"})

			err := cmd.Execute()

			require.NoError(t, err)
			var got reportv1alpha1.AcceleratorExplainReport
			require.NoError(t, json.Unmarshal(output.Bytes(), &got))
			assert.Equal(t, reportv1alpha1.AcceleratorClassInvalid, got.Content.Components[0].Class.State)
			assert.Contains(t, got.Content.Components[0].Issues, reportv1alpha1.AcceleratorIssueClassInvalid)
		})
	}
}

func TestExplainFormatsShareOneTypedProjection(t *testing.T) {
	formats := []string{"table", "wide", "json", "yaml"}
	for _, format := range formats {
		t.Run(format, func(t *testing.T) {
			isvc, runtimeObject, class := acceleratorCommandFixtures()
			f := factory.Static{
				OME: omefake.NewSimpleClientset(isvc, class), Kube: k8sfake.NewSimpleClientset(),
				Runtime: ctrlfake.NewClientBuilder().WithScheme(acceleratorScheme(t)).WithObjects(runtimeObject).Build(),
				NS:      "prod",
			}
			var output bytes.Buffer
			cmd := newExplainCmdWithDependencies(f, genericiooptions.IOStreams{
				In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{},
			}, fixedExplainDependencies())
			cmd.SetArgs([]string{"chat", "-o", format})

			require.NoError(t, cmd.Execute())
			assert.NotEmpty(t, output.String())
			assert.NotContains(t, output.String(), "resourceVersion")
			assert.NotContains(t, strings.ToLower(output.String()), "uid")
			switch format {
			case "table":
				assert.Contains(t, output.String(), "COMP")
				assert.Contains(t, output.String(), "gpu-a")
				for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
					assert.LessOrEqual(t, len([]rune(line)), 80, line)
				}
			case "wide":
				assert.Contains(t, output.String(), "Reported/Redacted")
				assert.Contains(t, output.String(), "EFFECTIVE")
			case "json":
				assert.Contains(t, output.String(), `"kind": "AcceleratorExplainReport"`)
			case "yaml":
				assert.Contains(t, output.String(), "kind: AcceleratorExplainReport")
			}
		})
	}
}

func TestExplainValidationAndWriterErrors(t *testing.T) {
	cmd := newExplainCmdWithDependencies(factory.Static{}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{},
	}, fixedExplainDependencies())
	cmd.SetArgs([]string{"INVALID_NAME"})
	require.Error(t, cmd.Execute())

	cmd = newExplainCmdWithDependencies(factory.Static{}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{},
	}, fixedExplainDependencies())
	cmd.SetArgs([]string{"chat", "-o", "xml"})
	require.Error(t, cmd.Execute())

	isvc, runtimeObject, class := acceleratorCommandFixtures()
	cmd = newExplainCmdWithDependencies(factory.Static{
		OME: omefake.NewSimpleClientset(isvc, class), Kube: k8sfake.NewSimpleClientset(),
		Runtime: ctrlfake.NewClientBuilder().WithScheme(acceleratorScheme(t)).WithObjects(runtimeObject).Build(),
		NS:      "prod",
	}, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: failingWriter{}, ErrOut: &bytes.Buffer{}}, fixedExplainDependencies())
	cmd.SetArgs([]string{"chat"})
	require.ErrorContains(t, cmd.Execute(), "write accelerator explain report")

	isvc, runtimeObject, class = acceleratorCommandFixtures()
	cmd = newExplainCmdWithDependencies(factory.Static{
		OME: omefake.NewSimpleClientset(isvc, class), Kube: k8sfake.NewSimpleClientset(),
		Runtime: ctrlfake.NewClientBuilder().WithScheme(acceleratorScheme(t)).
			WithObjects(runtimeObject).Build(),
		NS: "prod",
	}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: failingWriter{}, ErrOut: &bytes.Buffer{},
	}, fixedExplainDependencies())
	cmd.SetArgs([]string{"chat", "-o", "wide"})
	require.ErrorContains(t, cmd.Execute(), "write accelerator explain report")
}

func TestExplainRejectsUnboundPrimaryResponses(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
		want   error
	}{
		{name: "wrong name", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Name = "other"
		}, want: errReturnedInferenceServiceIdentity},
		{name: "wrong namespace", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Namespace = "other"
		}, want: errReturnedInferenceServiceIdentity},
		{name: "missing uid", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.UID = ""
		}, want: errReturnedInferenceServiceUnbound},
		{name: "missing resource version", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.ResourceVersion = ""
		}, want: errReturnedInferenceServiceUnbound},
		{name: "missing generation", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Generation = 0
		}, want: errReturnedInferenceServiceUnbound},
	} {
		t.Run(tt.name, func(t *testing.T) {
			isvc, _, _ := acceleratorCommandFixtures()
			tt.mutate(isvc)
			omeClient := omefake.NewSimpleClientset()
			omeClient.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, isvc.DeepCopy(), nil
			})
			f := factory.Static{OME: omeClient, NS: "prod"}
			cmd := newExplainCmdWithDependencies(f, genericiooptions.IOStreams{
				In: &bytes.Buffer{}, Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{},
			}, fixedExplainDependencies())
			cmd.SetArgs([]string{"chat"})

			err := cmd.Execute()

			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestExplainRunRejectsInvalidEvidenceAndProjectionFailures(t *testing.T) {
	isvc, _, _ := acceleratorCommandFixtures()
	projectionFailure := errors.New("projection failed")
	for _, tt := range []struct {
		name    string
		collect explainCollector
		project explainProjector
		want    error
	}{
		{name: "nil evidence", collect: func(context.Context, factory.Factory, *namespace.Options, string, paging.Limits) (*explainEvidence, error) {
			return nil, nil
		}, project: acceleratorprojection.Project, want: acceleratorprojection.ErrInvalidEvidence},
		{name: "missing primary", collect: func(context.Context, factory.Factory, *namespace.Options, string, paging.Limits) (*explainEvidence, error) {
			return &explainEvidence{}, nil
		}, project: acceleratorprojection.Project, want: acceleratorprojection.ErrInvalidEvidence},
		{name: "projector failure", collect: func(context.Context, factory.Factory, *namespace.Options, string, paging.Limits) (*explainEvidence, error) {
			return &explainEvidence{inferenceService: isvc}, nil
		}, project: func(*omev1beta1.InferenceService, effective.AcceleratorBaseResolution,
			map[string]acceleratorprojection.AcceleratorClassEvidence, reportv1alpha1.Clock,
		) (reportv1alpha1.AcceleratorExplainReport, error) {
			return reportv1alpha1.AcceleratorExplainReport{}, projectionFailure
		}, want: projectionFailure},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newExplainCmdWithDependencies(factory.Static{}, genericiooptions.IOStreams{
				In: &bytes.Buffer{}, Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{},
			}, explainDependencies{
				clock: fixedCommandClock(), limits: commandLimits(),
				collect: tt.collect, project: tt.project,
			})
			cmd.SetArgs([]string{"chat"})

			err := cmd.Execute()

			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestCollectAcceleratorEvidenceRejectsInvalidLimitsAndFactoryFailures(t *testing.T) {
	_, err := collectAcceleratorEvidence(
		context.Background(), &acceleratorTrackingFactory{namespace: "prod"},
		namespace.NewOptions(), "chat", paging.Limits{},
	)
	require.ErrorContains(t, err, "limits are invalid")

	namespaceFailure := errors.New("namespace failed")
	_, err = collectAcceleratorEvidence(
		context.Background(), &acceleratorTrackingFactory{namespaceErr: namespaceFailure},
		namespace.NewOptions(), "chat", commandLimits(),
	)
	require.ErrorIs(t, err, namespaceFailure)

	clientFailure := errors.New("OME client failed")
	_, err = collectAcceleratorEvidence(
		context.Background(), &acceleratorTrackingFactory{namespace: "prod", omeErr: clientFailure},
		namespace.NewOptions(), "chat", commandLimits(),
	)
	require.ErrorIs(t, err, clientFailure)

	isvc, _, _ := acceleratorCommandFixtures()
	kubeFailure := errors.New("Kubernetes client failed")
	_, err = collectAcceleratorEvidence(
		context.Background(), &acceleratorTrackingFactory{
			namespace: "prod", ome: omefake.NewSimpleClientset(isvc), kubeErr: kubeFailure,
		}, namespace.NewOptions(), "chat", commandLimits(),
	)
	require.ErrorIs(t, err, kubeFailure)

	runtimeFailure := errors.New("runtime client failed")
	_, err = collectAcceleratorEvidence(
		context.Background(), &acceleratorTrackingFactory{
			namespace: "prod", ome: omefake.NewSimpleClientset(isvc),
			kube: k8sfake.NewSimpleClientset(), runtimeErr: runtimeFailure,
		}, namespace.NewOptions(), "chat", commandLimits(),
	)
	require.ErrorIs(t, err, runtimeFailure)

	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = collectAcceleratorEvidence(
		canceledContext, &acceleratorTrackingFactory{namespace: "prod"},
		namespace.NewOptions(), "chat", commandLimits(),
	)
	require.ErrorIs(t, err, context.Canceled)
}

func acceleratorCommandFixtures() (*omev1beta1.InferenceService, *omev1beta1.ServingRuntime, *omev1beta1.AcceleratorClass) {
	autoSync := true
	kind := runtimeselector.KindServingRuntime
	isvc := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("isvc-uid"), ResourceVersion: "17", Generation: 4,
	}, Spec: omev1beta1.InferenceServiceSpec{
		Runtime:             &omev1beta1.ServingRuntimeRef{Name: "runtime", Kind: &kind, AutoSync: &autoSync},
		AcceleratorSelector: &omev1beta1.AcceleratorSelector{Policy: omev1beta1.CheapestPolicy},
		Engine:              &omev1beta1.EngineSpec{Runner: &omev1beta1.RunnerSpec{}},
	}}
	isvc.Status.ObservedGeneration = 4
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {SelectedAccelerator: &omev1beta1.AcceleratorSelection{
			AcceleratorClass: "gpu-a", Reason: "selected by policy",
			ResourceRequests: map[string]string{"example.com/gpu": "1"},
		}},
	}
	runtimeObject := &omev1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{
		Name: "runtime", Namespace: "prod", UID: types.UID("runtime-uid"), ResourceVersion: "29", Generation: 2,
	}, Spec: omev1beta1.ServingRuntimeSpec{
		EngineConfig:            &omev1beta1.EngineSpec{},
		AcceleratorRequirements: &omev1beta1.AcceleratorRequirements{AcceleratorClasses: []string{"gpu-a"}},
	}}
	class := &omev1beta1.AcceleratorClass{ObjectMeta: metav1.ObjectMeta{
		Name: "gpu-a", UID: types.UID("class-uid"), ResourceVersion: "9", Generation: 2,
	}}
	return isvc, runtimeObject, class
}

func acceleratorScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(appsv1.AddToScheme(scheme))
	utilruntime.Must(omev1beta1.AddToScheme(scheme))
	return scheme
}

func commandLimits() paging.Limits {
	return paging.Limits{PageSize: 500, MaxItems: 1000, MaxPages: 2, RequestTimeout: 10 * time.Second}
}

func fixedCommandClock() reportv1alpha1.Clock {
	return reportv1alpha1.ClockFunc(func() time.Time {
		return time.Date(2026, 9, 14, 20, 30, 0, 0, time.UTC)
	})
}

func fixedExplainDependencies() explainDependencies {
	return explainDependencies{
		clock: fixedCommandClock(), limits: commandLimits(),
		collect: collectAcceleratorEvidence, project: acceleratorprojection.Project,
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

type acceleratorDeadlineObservation struct {
	kind        string
	deadline    time.Time
	hasDeadline bool
}

type acceleratorDeadlineOMEClient struct {
	versioned.Interface
	reads []acceleratorDeadlineObservation
}

func (c *acceleratorDeadlineOMEClient) OmeV1beta1() ometypedv1beta1.OmeV1beta1Interface {
	return &acceleratorDeadlineOmeV1beta1{
		OmeV1beta1Interface: c.Interface.OmeV1beta1(), parent: c,
	}
}

type acceleratorDeadlineOmeV1beta1 struct {
	ometypedv1beta1.OmeV1beta1Interface
	parent *acceleratorDeadlineOMEClient
}

func (c *acceleratorDeadlineOmeV1beta1) InferenceServices(
	namespace string,
) ometypedv1beta1.InferenceServiceInterface {
	return &acceleratorDeadlineInferenceServices{
		InferenceServiceInterface: c.OmeV1beta1Interface.InferenceServices(namespace),
		parent:                    c.parent,
	}
}

func (c *acceleratorDeadlineOmeV1beta1) AcceleratorClasses() ometypedv1beta1.AcceleratorClassInterface {
	return &acceleratorDeadlineAcceleratorClasses{
		AcceleratorClassInterface: c.OmeV1beta1Interface.AcceleratorClasses(),
		parent:                    c.parent,
	}
}

type acceleratorDeadlineInferenceServices struct {
	ometypedv1beta1.InferenceServiceInterface
	parent *acceleratorDeadlineOMEClient
}

func (c *acceleratorDeadlineInferenceServices) Get(
	ctx context.Context,
	name string,
	options metav1.GetOptions,
) (*omev1beta1.InferenceService, error) {
	c.parent.observe("InferenceService", ctx)
	return c.InferenceServiceInterface.Get(ctx, name, options)
}

type acceleratorDeadlineAcceleratorClasses struct {
	ometypedv1beta1.AcceleratorClassInterface
	parent *acceleratorDeadlineOMEClient
}

func (c *acceleratorDeadlineAcceleratorClasses) Get(
	ctx context.Context,
	name string,
	options metav1.GetOptions,
) (*omev1beta1.AcceleratorClass, error) {
	c.parent.observe("AcceleratorClass", ctx)
	return c.AcceleratorClassInterface.Get(ctx, name, options)
}

func (c *acceleratorDeadlineOMEClient) observe(kind string, ctx context.Context) {
	deadline, ok := ctx.Deadline()
	c.reads = append(c.reads, acceleratorDeadlineObservation{
		kind: kind, deadline: deadline, hasDeadline: ok,
	})
}

type acceleratorTrackingFactory struct {
	ome          versioned.Interface
	kube         kubernetes.Interface
	runtime      ctrlclient.Client
	namespace    string
	namespaceErr error
	omeErr       error
	kubeErr      error
	runtimeErr   error
}

func (*acceleratorTrackingFactory) RESTConfig() (*rest.Config, error) {
	panic("unexpected RESTConfig call")
}

func (f *acceleratorTrackingFactory) Namespace() (string, bool, error) {
	return f.namespace, false, f.namespaceErr
}

func (f *acceleratorTrackingFactory) OMEClient() (versioned.Interface, error) {
	return f.ome, f.omeErr
}

func (f *acceleratorTrackingFactory) KubeClient() (kubernetes.Interface, error) {
	return f.kube, f.kubeErr
}

func (f *acceleratorTrackingFactory) RuntimeClient() (ctrlclient.Client, error) {
	return f.runtime, f.runtimeErr
}
