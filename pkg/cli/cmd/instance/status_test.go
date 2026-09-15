package instance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/instancecollection"
	"sigs.k8s.io/ome/pkg/cli/instancestatusprojection"
	"sigs.k8s.io/ome/pkg/cli/observation"
	"sigs.k8s.io/ome/pkg/cli/paging"
	versioned "sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/runtimerevision"
)

func TestStatusReadsExactBoundedSourcesAndRendersUsefulTable(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	ir := commandIR(isvc)
	pod := commandStatusPod(isvc, ir, "chat-engine-0")
	events := []corev1.Event{
		commandStatusEvent("pod-warning", "Pod", pod.Name, pod.UID),
	}
	kube := kubefake.NewSimpleClientset(&pod)
	kube.PrependReactor("list", "events", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.EventList{Items: events}, nil
	})
	ome := omefake.NewSimpleClientset(isvc, ir)

	out, err := executeStatus(t, factory.Static{OME: ome, Kube: kube, NS: "prod"}, statusCommandDependencies(), "chat", "0", "--component", "engine")

	require.NoError(t, err)
	for _, want := range []string{"FIELD", "VALUE", "Reported", "chat-engine", pod.Name, "FailedMount"} {
		assert.Contains(t, out, want)
	}
	require.Len(t, ome.Actions(), 2)
	assert.Equal(t, "get", ome.Actions()[0].GetVerb())
	assert.Equal(t, "list", ome.Actions()[1].GetVerb())
	podAction := firstCoreAction(t, kube.Actions(), "pods")
	podOptions := podAction.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
	for key, value := range map[string]string{
		constants.InferenceServicePodLabelKey: "chat", constants.OMEComponentLabel: "engine",
		query.LabelManagedBy: query.ManagedByOMENative, query.LabelInstanceIdx: "0",
	} {
		assert.Contains(t, podOptions.LabelSelector, key+"="+value)
	}
	assert.Equal(t, int64(4), podOptions.Limit)
	eventRequests := 0
	for _, action := range kube.Actions() {
		if action.GetResource().Resource == "events" {
			eventRequests++
		}
	}
	assert.Equal(t, 1, eventRequests)
}

func TestStatusValidatesArgumentsComponentIndexAndOutputBeforeFactoryAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing args", args: nil, want: "accepts 2 arg(s), received 0"},
		{name: "extra args", args: []string{"chat", "0", "extra", "--component", "engine"}, want: "accepts 2 arg(s), received 3"},
		{name: "missing component", args: []string{"chat", "0"}, want: ErrStatusComponentRequired.Error()},
		{name: "bad component", args: []string{"chat", "0", "--component", "future"}, want: ErrStatusComponentInvalid.Error()},
		{name: "negative", args: []string{"--component", "engine", "--", "chat", "-1"}, want: ErrStatusIndexInvalid.Error()},
		{name: "overflow", args: []string{"chat", "2147483648", "--component", "engine"}, want: ErrStatusIndexInvalid.Error()},
		{name: "not number", args: []string{"chat", "x", "--component", "engine"}, want: ErrStatusIndexInvalid.Error()},
		{name: "bad name", args: []string{"Bad_Name", "0", "--component", "engine"}, want: ErrInvalidInferenceServiceName.Error()},
		{name: "bad output", args: []string{"chat", "0", "--component", "engine", "-o", "csv"}, want: `unsupported output format "csv"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := executeStatus(t, panicFactory{}, statusCommandDependencies(), test.args...)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.want)
		})
	}
}

func TestStatusAcceptsMinAndMaxInt32AndWritesTypedJSONYAMLWithoutAliases(t *testing.T) {
	t.Parallel()

	for _, index := range []int32{0, math.MaxInt32} {
		for _, format := range []string{"json", "yaml"} {
			t.Run(format+"/"+strconv.FormatInt(int64(index), 10), func(t *testing.T) {
				isvc := commandISVC()
				ir := commandIR(isvc)
				ir.Status.InstanceStatuses[0].Index = index
				client := omefake.NewSimpleClientset(isvc, ir)
				kube := kubefake.NewSimpleClientset()
				out, err := executeStatus(t, factory.Static{OME: client, Kube: kube, NS: "prod"}, statusCommandDependencies(), "chat", strconv.FormatInt(int64(index), 10), "--component", "engine", "-o", format)
				require.NoError(t, err)
				assert.Contains(t, out, "cli.ome.io/v1alpha1")
				assert.Contains(t, out, "InstanceStatusReport")
				assert.NotContains(t, out, "resourceVersion")
				assert.NotContains(t, out, "annotations")
				if format == "json" {
					var decoded map[string]any
					require.NoError(t, json.Unmarshal([]byte(out), &decoded))
					assert.Equal(t, "InstanceStatusReport", decoded["kind"])
				}
			})
		}
	}
	cmd := newStatusCmdWithDependencies(panicFactory{}, genericiooptions.IOStreams{}, statusCommandDependencies())
	assert.Empty(t, cmd.Aliases)
}

func TestStatusReportsRawDeploymentWithoutListingIRPodsOrEvents(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	isvc.Spec.Engine = &omev1beta1.EngineSpec{ComponentExtensionSpec: omev1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}}}
	ome := omefake.NewSimpleClientset(isvc)

	out, err := executeStatus(t, factory.Static{OME: ome, NS: "prod"}, statusCommandDependencies(), "chat", "0", "--component", "engine", "-o", "json")

	require.NoError(t, err)
	assert.Contains(t, out, `"state": "NotOMENative"`)
	require.Len(t, ome.Actions(), 1)
	assert.Equal(t, "get", ome.Actions()[0].GetVerb())
}

func TestStatusEffectiveRuntimeComponentModeOverridesServiceSpecBothWays(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, runtimeMode string
		serviceMode       constants.DeploymentModeType
		wantState         string
	}{
		{name: "runtime native overrides service raw", runtimeMode: string(constants.OMENative), serviceMode: constants.RawDeployment, wantState: "Reported"},
		{name: "runtime raw overrides service native", runtimeMode: string(constants.RawDeployment), serviceMode: constants.OMENative, wantState: "NotOMENative"},
	} {
		t.Run(test.name, func(t *testing.T) {
			isvc := commandISVC()
			kind := "ServingRuntime"
			isvc.Spec.Runtime = &omev1beta1.ServingRuntimeRef{Name: "runtime", Kind: &kind}
			isvc.Spec.DeploymentMode = &test.serviceMode
			isvc.Spec.Engine = &omev1beta1.EngineSpec{}
			ir := commandIR(isvc)
			scheme := runtime.NewScheme()
			require.NoError(t, omev1beta1.AddToScheme(scheme))
			runtimeObject := &omev1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime", Namespace: "prod", UID: "runtime-uid", ResourceVersion: "1"}, Spec: omev1beta1.ServingRuntimeSpec{
				EngineConfig: &omev1beta1.EngineSpec{ComponentExtensionSpec: omev1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: test.runtimeMode}}},
			}}
			ctrl := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(runtimeObject).Build()
			out, err := executeStatus(t, factory.Static{OME: omefake.NewSimpleClientset(isvc, ir), Kube: kubefake.NewSimpleClientset(), Runtime: ctrl, NS: "prod"}, statusCommandDependencies(), "chat", "0", "--component", "engine", "-o", "json")
			require.NoError(t, err)
			assert.Contains(t, out, `"state": "`+test.wantState+`"`)
			assert.Contains(t, out, `"mode": "`+test.runtimeMode+`"`)
			assert.Contains(t, out, `"source": "ComponentAnnotation"`)
		})
	}
}

func TestStatusEffectiveRuntimeComponentModeOverridesTypedVirtualServiceSpec(t *testing.T) {
	t.Parallel()
	isvc := commandISVC()
	kind := "ServingRuntime"
	mode := constants.VirtualDeployment
	isvc.Spec.Runtime = &omev1beta1.ServingRuntimeRef{Name: "runtime", Kind: &kind}
	isvc.Spec.DeploymentMode = &mode
	isvc.Spec.Engine = &omev1beta1.EngineSpec{}
	ir := commandIR(isvc)
	pod := commandStatusPod(isvc, ir, "selected")
	scheme := runtime.NewScheme()
	require.NoError(t, omev1beta1.AddToScheme(scheme))
	runtimeObject := &omev1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "runtime", Namespace: "prod", UID: "runtime-uid", ResourceVersion: "1"},
		Spec:       statusRuntimeSpec(string(constants.OMENative)),
	}
	ctrl := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(runtimeObject).Build()
	ome := omefake.NewSimpleClientset(isvc, ir)
	kube := kubefake.NewSimpleClientset(&pod)

	out, err := executeStatus(t, factory.Static{OME: ome, Kube: kube, Runtime: ctrl, NS: "prod"}, statusCommandDependencies(), "chat", "0", "--component", "engine", "-o", "json")

	require.NoError(t, err)
	assert.Contains(t, out, `"state": "Reported"`)
	assert.Contains(t, out, `"mode": "OMENative"`)
	assert.Contains(t, out, `"source": "ComponentAnnotation"`)
	assert.Contains(t, out, `"name": "`+pod.Name+`"`)
	require.Len(t, ome.Actions(), 2)
	assert.Equal(t, "list", ome.Actions()[1].GetVerb())
}

func TestStatusUsesPinnedRuntimeAndExplicitControlPlaneNamespace(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, liveMode, pinnedMode, wantState string
		livePresent                           bool
	}{
		{name: "pinned native beats live raw", liveMode: string(constants.RawDeployment), pinnedMode: string(constants.OMENative), wantState: "Reported", livePresent: true},
		{name: "pinned raw beats live native", liveMode: string(constants.OMENative), pinnedMode: string(constants.RawDeployment), wantState: "NotOMENative", livePresent: true},
		{name: "deleted live runtime retains valid pin", pinnedMode: string(constants.OMENative), wantState: "Reported"},
	} {
		t.Run(test.name, func(t *testing.T) {
			isvc := commandISVC()
			kind, autoSync := "ServingRuntime", false
			isvc.Spec.Runtime = &omev1beta1.ServingRuntimeRef{Name: "runtime", Kind: &kind, AutoSync: &autoSync}
			isvc.Spec.Engine = &omev1beta1.EngineSpec{}
			live := &omev1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime", Namespace: "prod", UID: "runtime-uid"}, Spec: statusRuntimeSpec(test.liveMode)}
			pinned := statusRuntimeRevision(t, "control-plane", "runtime", statusRuntimeSpec(test.pinnedMode))
			isvc.Status.PinnedRevisionName = pinned.Name
			ir := commandIR(isvc)
			scheme := runtime.NewScheme()
			require.NoError(t, omev1beta1.AddToScheme(scheme))
			builder := ctrlfake.NewClientBuilder().WithScheme(scheme)
			if test.livePresent {
				builder = builder.WithObjects(live)
			}
			ctrl := builder.Build()
			kube := kubefake.NewSimpleClientset(pinned)
			out, err := executeStatus(t, factory.Static{OME: omefake.NewSimpleClientset(isvc, ir), Kube: kube, Runtime: ctrl, NS: "prod"}, statusCommandDependencies(), "chat", "0", "--component", "engine", "--ome-namespace", "control-plane", "-o", "json")
			require.NoError(t, err)
			assert.Contains(t, out, `"state": "`+test.wantState+`"`)
			assert.Contains(t, out, `"mode": "`+test.pinnedMode+`"`)
			assert.Contains(t, out, `"origin": "ControllerRevision"`)
			get := firstCoreAction(t, kube.Actions(), "controllerrevisions")
			assert.Equal(t, "control-plane", get.GetNamespace())
		})
	}
}

func TestStatusClassifiesInvalidPinnedRevisionEvidence(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, want string
		mutate     func(*appsv1.ControllerRevision)
		missing    bool
	}{
		{name: "missing", want: "NotFound", missing: true},
		{name: "malformed", want: "MalformedPayload", mutate: func(revision *appsv1.ControllerRevision) { revision.Data.Raw = []byte(`{`) }},
		{name: "disabled", want: "Disabled", mutate: func(revision *appsv1.ControllerRevision) {
			spec := statusRuntimeSpec(string(constants.OMENative))
			spec.Disabled = boolPointer(true)
			revision.Data.Raw, _ = json.Marshal(&spec)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			isvc := commandISVC()
			kind, autoSync, requested := "ServingRuntime", false, ""
			isvc.Spec.Runtime = &omev1beta1.ServingRuntimeRef{Name: "runtime", Kind: &kind, AutoSync: &autoSync, Revision: &requested}
			isvc.Spec.Engine = &omev1beta1.EngineSpec{}
			live := &omev1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime", Namespace: "prod", UID: "runtime-uid"}, Spec: statusRuntimeSpec(string(constants.OMENative))}
			revision := statusRuntimeRevision(t, "ome", "runtime", statusRuntimeSpec(string(constants.OMENative)))
			requested = revision.Name
			if test.mutate != nil {
				test.mutate(revision)
			}
			objects := []runtime.Object{}
			if !test.missing {
				objects = append(objects, revision)
			}
			scheme := runtime.NewScheme()
			require.NoError(t, omev1beta1.AddToScheme(scheme))
			ctrl := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(live).Build()
			out, err := executeStatus(t, factory.Static{OME: omefake.NewSimpleClientset(isvc, commandIR(isvc)), Kube: kubefake.NewSimpleClientset(objects...), Runtime: ctrl, NS: "prod"}, statusCommandDependencies(), "chat", "0", "--component", "engine", "-o", "json")
			require.NoError(t, err)
			assert.Contains(t, out, `"unavailableReason": "`+test.want+`"`)
		})
	}
}

func TestStatusResolutionUnavailableKeepsExactCurrentIRAndReportsModeEvidence(t *testing.T) {
	t.Parallel()
	isvc := commandISVC()
	unknown := constants.DeploymentModeType("Future")
	isvc.Spec.DeploymentMode = &unknown
	ir := commandIR(isvc)
	out, err := executeStatus(t, factory.Static{OME: omefake.NewSimpleClientset(isvc, ir), Kube: kubefake.NewSimpleClientset(), NS: "prod"}, statusCommandDependencies(), "chat", "0", "--component", "engine", "-o", "json")
	require.NoError(t, err)
	assert.Contains(t, out, `"state": "Reported"`)
	assert.Contains(t, out, `"deployment": {`)
	assert.Contains(t, out, `"evidence": "Unavailable"`)
}

func TestStatusClassifiesMissingDisabledAndCyclicRuntimeEvidence(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, reason string
		objects      []ctrlclient.Object
	}{
		{name: "missing", reason: "NotFound"},
		{name: "disabled", reason: "Disabled", objects: []ctrlclient.Object{&omev1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime", Namespace: "prod", UID: "runtime-uid"}, Spec: omev1beta1.ServingRuntimeSpec{Disabled: boolPointer(true)}}}},
		{name: "cycle", reason: "Cycle", objects: []ctrlclient.Object{
			&omev1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "runtime", Namespace: "prod", UID: "runtime-uid", Annotations: map[string]string{constants.RuntimeInheritFromAnnotationKey: "parent"}}, Spec: statusRuntimeSpec(string(constants.OMENative))},
			&omev1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "parent", Namespace: "prod", UID: "parent-uid", Annotations: map[string]string{constants.RuntimeInheritFromAnnotationKey: "runtime"}}, Spec: statusRuntimeSpec(string(constants.OMENative))},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			isvc := commandISVC()
			kind := "ServingRuntime"
			isvc.Spec.Runtime = &omev1beta1.ServingRuntimeRef{Name: "runtime", Kind: &kind}
			isvc.Spec.Engine = &omev1beta1.EngineSpec{}
			ir := commandIR(isvc)
			scheme := runtime.NewScheme()
			require.NoError(t, omev1beta1.AddToScheme(scheme))
			ctrl := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(test.objects...).Build()
			out, err := executeStatus(t, factory.Static{OME: omefake.NewSimpleClientset(isvc, ir), Kube: kubefake.NewSimpleClientset(), Runtime: ctrl, NS: "prod"}, statusCommandDependencies(), "chat", "0", "--component", "engine", "-o", "json")
			require.NoError(t, err)
			assert.Contains(t, out, `"unavailableReason": "`+test.reason+`"`)
			assert.Contains(t, out, `"inferenceReplica": "chat-engine"`)
		})
	}
}

func TestStatusClassifiesRuntimeInheritanceMaxDepth(t *testing.T) {
	t.Parallel()
	isvc := commandISVC()
	kind := "ServingRuntime"
	isvc.Spec.Runtime = &omev1beta1.ServingRuntimeRef{Name: "runtime-00", Kind: &kind}
	isvc.Spec.Engine = &omev1beta1.EngineSpec{}
	objects := make([]ctrlclient.Object, 40)
	for i := range objects {
		name := fmt.Sprintf("runtime-%02d", i)
		annotations := map[string]string{}
		if i+1 < len(objects) {
			annotations[constants.RuntimeInheritFromAnnotationKey] = fmt.Sprintf("runtime-%02d", i+1)
		}
		objects[i] = &omev1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod", UID: types.UID("uid-" + name), Annotations: annotations}, Spec: statusRuntimeSpec(string(constants.OMENative))}
	}
	scheme := runtime.NewScheme()
	require.NoError(t, omev1beta1.AddToScheme(scheme))
	ctrl := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	out, err := executeStatus(t, factory.Static{OME: omefake.NewSimpleClientset(isvc, commandIR(isvc)), Kube: kubefake.NewSimpleClientset(), Runtime: ctrl, NS: "prod"}, statusCommandDependencies(), "chat", "0", "--component", "engine", "-o", "json")
	require.NoError(t, err)
	assert.Contains(t, out, `"unavailableReason": "MaxDepthExceeded"`)
}

func TestStatusWideOutputAndWriterFailure(t *testing.T) {
	t.Parallel()
	isvc := commandISVC()
	isvc.Spec.Engine = &omev1beta1.EngineSpec{ComponentExtensionSpec: omev1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)}}}
	ir := commandIR(isvc)
	operationTime := metav1.NewTime(time.Date(2026, 9, 14, 20, 0, 0, 0, time.UTC))
	ir.Status.InstanceStatuses[0].Operation = &omev1beta1.InstanceOperation{ID: "wide-operation", Type: omev1beta1.InstanceOperationMigrate, Step: "Move", FromNode: "node-old", StartedAt: operationTime, LastProgressAt: operationTime}
	out, err := executeStatus(t, factory.Static{OME: omefake.NewSimpleClientset(isvc, ir), Kube: kubefake.NewSimpleClientset(), NS: "prod"}, statusCommandDependencies(), "chat", "0", "--component", "engine", "-o", "wide")
	require.NoError(t, err)
	assert.Contains(t, out, "wide-operation")
	assert.Contains(t, out, "running revision")

	want := errors.New("wide writer failed")
	cmd := newStatusCmdWithDependencies(factory.Static{OME: omefake.NewSimpleClientset(isvc, ir), Kube: kubefake.NewSimpleClientset(), NS: "prod"}, genericiooptions.IOStreams{Out: errorWriter{err: want}}, statusCommandDependencies())
	cmd.SetArgs([]string{"chat", "0", "--component", "engine", "-o", "wide"})
	assert.ErrorIs(t, cmd.Execute(), want)
}

func TestStatusServiceVirtualAnnotationOverridesComponentOMENative(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	isvc.Annotations = map[string]string{}
	isvc.Annotations[constants.DeploymentMode] = string(constants.VirtualDeployment)
	isvc.Spec.Engine = &omev1beta1.EngineSpec{}
	isvc.Spec.Engine.Annotations = map[string]string{constants.DeploymentMode: string(constants.OMENative)}
	ome := omefake.NewSimpleClientset(isvc)

	out, err := executeStatus(t, factory.Static{OME: ome, NS: "prod"}, statusCommandDependencies(), "chat", "0", "--component", "engine", "-o", "json")

	require.NoError(t, err)
	assert.Contains(t, out, `"state": "NotOMENative"`)
	require.Len(t, ome.Actions(), 1)
}

func TestStatusRepresentsCollectionPodAndEventFailuresWithoutLosingAuthority(t *testing.T) {
	t.Parallel()

	t.Run("IR collection forbidden", func(t *testing.T) {
		isvc := commandISVC()
		ome := omefake.NewSimpleClientset(isvc)
		ome.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "inferencereplicas"}, "", errors.New("SECRET denied"))
		})
		out, err := executeStatus(t, factory.Static{OME: ome, NS: "prod"}, statusCommandDependencies(), "chat", "0", "--component", "engine", "-o", "json")
		require.NoError(t, err)
		assert.Contains(t, out, `"state": "Unavailable"`)
		assert.Contains(t, out, `"unavailableReason": "Forbidden"`)
		assert.NotContains(t, out, "SECRET")
	})

	t.Run("pods forbidden", func(t *testing.T) {
		isvc := commandISVC()
		ir := commandIR(isvc)
		kube := kubefake.NewSimpleClientset()
		kube.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", errors.New("SECRET denied"))
		})
		out, err := executeStatus(t, factory.Static{OME: omefake.NewSimpleClientset(isvc, ir), Kube: kube, NS: "prod"}, statusCommandDependencies(), "chat", "0", "--component", "engine", "-o", "json")
		require.NoError(t, err)
		assert.Contains(t, out, `"state": "Partial"`)
		assert.Contains(t, out, `"code": "PodsUnavailable"`)
		assert.Contains(t, out, `"inferenceReplica": "chat-engine"`)
		assert.NotContains(t, out, "SECRET")
	})

	t.Run("events forbidden", func(t *testing.T) {
		isvc := commandISVC()
		ir := commandIR(isvc)
		pod := commandStatusPod(isvc, ir, "chat-engine-0")
		kube := kubefake.NewSimpleClientset(&pod)
		kube.PrependReactor("list", "events", func(ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "events"}, "", errors.New("SECRET denied"))
		})
		out, err := executeStatus(t, factory.Static{OME: omefake.NewSimpleClientset(isvc, ir), Kube: kube, NS: "prod"}, statusCommandDependencies(), "chat", "0", "--component", "engine", "-o", "json")
		require.NoError(t, err)
		assert.Contains(t, out, `"code": "EventsUnavailable"`)
		assert.Contains(t, out, `"name": "`+pod.Name+`"`)
		assert.NotContains(t, out, "SECRET")
	})
}

func TestStatusIncompleteIRPaginationNeverJoinsLiveSources(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		failSecond bool
		want       string
	}{
		{name: "later page forbidden", failSecond: true, want: `"unavailableReason": "Forbidden"`},
		{name: "page cap", want: `"code": "CollectionTruncated"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			isvc := commandISVC()
			ir := commandIR(isvc)
			ome := omefake.NewSimpleClientset(isvc)
			calls := 0
			ome.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
				calls++
				if calls == 2 && test.failSecond {
					return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "inferencereplicas"}, "", errors.New("hidden"))
				}
				return true, &omev1beta1.InferenceReplicaList{ListMeta: metav1.ListMeta{Continue: "next"}, Items: []omev1beta1.InferenceReplica{*ir}}, nil
			})
			deps := statusCommandDependencies()
			if !test.failSecond {
				deps.irLimits.Paging.MaxPages = 1
			}
			kube := kubefake.NewSimpleClientset()
			out, err := executeStatus(t, factory.Static{OME: ome, Kube: kube, NS: "prod"}, deps, "chat", "0", "--component", "engine", "-o", "json")
			require.NoError(t, err)
			assert.Contains(t, out, `"state": "Unavailable"`)
			assert.Contains(t, out, test.want)
			assert.Empty(t, kube.Actions())
		})
	}
}

func TestStatusReturnsParentCancellationAndWriterErrors(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	ome := omefake.NewSimpleClientset(isvc)
	ome.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, context.Canceled
	})
	_, err := executeStatus(t, factory.Static{OME: ome, NS: "prod"}, statusCommandDependencies(), "chat", "0", "--component", "engine")
	assert.ErrorIs(t, err, context.Canceled)

	want := errors.New("writer failed")
	isvc = commandISVC()
	isvc.Spec.Engine = &omev1beta1.EngineSpec{ComponentExtensionSpec: omev1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}}}
	cmd := newStatusCmdWithDependencies(factory.Static{OME: omefake.NewSimpleClientset(isvc), NS: "prod"}, genericiooptions.IOStreams{Out: errorWriter{err: want}}, statusCommandDependencies())
	cmd.SetArgs([]string{"chat", "0", "--component", "engine"})
	err = cmd.Execute()
	require.ErrorIs(t, err, want)
	assert.Contains(t, err.Error(), "write instance status")
}

func TestStatusReturnsParentAndFactoryErrors(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	tests := []struct {
		name    string
		factory factory.Factory
		want    string
	}{
		{name: "namespace", factory: namespaceFactory{err: boom}, want: "resolve namespace"},
		{name: "invalid namespace", factory: namespaceFactory{namespace: "INVALID"}, want: ErrInvalidNamespace.Error()},
		{name: "OME client", factory: statusFactory{ns: "prod", omeErr: boom}, want: "create OME client"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := executeStatus(t, test.factory, statusCommandDependencies(), "chat", "0", "--component", "engine")
			require.ErrorContains(t, err, test.want)
		})
	}

	isvc := commandISVC()
	kubeFactory := statusFactory{ns: "prod", ome: omefake.NewSimpleClientset(isvc, commandIR(isvc)), kubeErr: boom}
	out, err := executeStatus(t, kubeFactory, statusCommandDependencies(), "chat", "0", "--component", "engine", "-o", "json")
	require.NoError(t, err)
	assert.Contains(t, out, `"code": "PodsUnavailable"`)
	assert.Contains(t, out, `"code": "EventsUnavailable"`)
}

func TestStatusRejectsHostileParentGetIdentities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		object *omev1beta1.InferenceService
		want   error
	}{
		{name: "nil", object: nil, want: ErrReturnedInferenceServiceNil},
		{name: "wrong name", object: func() *omev1beta1.InferenceService { value := commandISVC(); value.Name = "other"; return value }(), want: ErrReturnedInferenceServiceNameMismatch},
		{name: "wrong namespace", object: func() *omev1beta1.InferenceService { value := commandISVC(); value.Namespace = "other"; return value }(), want: ErrReturnedInferenceServiceNamespaceMismatch},
		{name: "missing UID", object: func() *omev1beta1.InferenceService { value := commandISVC(); value.UID = ""; return value }(), want: ErrReturnedInferenceServiceUIDMissing},
		{name: "unsafe UID", object: func() *omev1beta1.InferenceService { value := commandISVC(); value.UID = "uid/unsafe"; return value }(), want: ErrReturnedInferenceServiceUIDInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := omefake.NewSimpleClientset()
			client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, test.object, nil
			})
			_, err := executeStatus(t, factory.Static{OME: client, NS: "prod"}, statusCommandDependencies(), "chat", "0", "--component", "engine")
			assert.ErrorIs(t, err, test.want)
		})
	}
}

func TestStatusMapsPodNotFoundAndProjectionFailures(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	ir := commandIR(isvc)
	kube := kubefake.NewSimpleClientset()
	kube.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "selected")
	})
	out, err := executeStatus(t, factory.Static{OME: omefake.NewSimpleClientset(isvc, ir), Kube: kube, NS: "prod"}, statusCommandDependencies(), "chat", "0", "--component", "engine", "-o", "json")
	require.NoError(t, err)
	assert.Contains(t, out, `"unavailableReason": "NotFound"`)

	mode := constants.RawDeployment
	isvc.Spec.DeploymentMode = &mode
	deps := statusCommandDependencies()
	deps.projectionLimits.MaxPods = 0
	_, err = executeStatus(t, factory.Static{OME: omefake.NewSimpleClientset(isvc), NS: "prod"}, deps, "chat", "0", "--component", "engine")
	require.ErrorContains(t, err, "project instance status")
}

func TestStatusHelpDefinesFieldsBoundsAndReadOnlyBehavior(t *testing.T) {
	t.Parallel()

	cmd := newStatusCmdWithDependencies(panicFactory{}, genericiooptions.IOStreams{}, statusCommandDependencies())
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--help"})
	require.NoError(t, cmd.Execute())
	for _, want := range []string{
		"IR status is authoritative", "serving readiness gate", "Event messages are never shown",
		"POD restarts", "table, wide, json or yaml", "read-only", "--ome-namespace",
		"ControllerRevision", "migration", "encoding",
	} {
		assert.Contains(t, out.String(), want)
	}
}

func TestStatusDefinitiveDeploymentModeResolutionIsFailClosed(t *testing.T) {
	t.Parallel()

	raw, native := constants.RawDeployment, constants.OMENative
	tests := []struct {
		name      string
		component omev1beta1.ComponentType
		mutate    func(*omev1beta1.InferenceService)
		want      bool
	}{
		{name: "typed raw requires runtime merge", component: omev1beta1.EngineComponent, mutate: func(isvc *omev1beta1.InferenceService) { isvc.Spec.DeploymentMode = &raw }, want: false},
		{name: "typed native", component: omev1beta1.EngineComponent, mutate: func(isvc *omev1beta1.InferenceService) { isvc.Spec.DeploymentMode = &native }, want: false},
		{name: "typed virtual requires runtime merge", component: omev1beta1.EngineComponent, mutate: func(isvc *omev1beta1.InferenceService) {
			mode := constants.VirtualDeployment
			isvc.Spec.DeploymentMode = &mode
		}, want: false},
		{name: "engine annotation raw", component: omev1beta1.EngineComponent, mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Spec.Engine = &omev1beta1.EngineSpec{ComponentExtensionSpec: omev1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: string(raw)}}}
		}, want: true},
		{name: "decoder annotation raw", component: omev1beta1.DecoderComponent, mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Spec.Decoder = &omev1beta1.DecoderSpec{ComponentExtensionSpec: omev1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: string(raw)}}}
		}, want: true},
		{name: "router annotation raw", component: omev1beta1.RouterComponent, mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Spec.Router = &omev1beta1.RouterSpec{ComponentExtensionSpec: omev1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: string(raw)}}}
		}, want: true},
		{name: "service virtual wins", component: omev1beta1.EngineComponent, mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Annotations = map[string]string{constants.DeploymentMode: string(constants.VirtualDeployment)}
		}, want: true},
		{name: "service raw is not component authority", component: omev1beta1.EngineComponent, mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Annotations = map[string]string{constants.DeploymentMode: string(raw)}
		}, want: false},
		{name: "malformed annotation ignored", component: omev1beta1.EngineComponent, mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Spec.Engine = &omev1beta1.EngineSpec{ComponentExtensionSpec: omev1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: "Future"}}}
		}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isvc := commandISVC()
			test.mutate(isvc)
			_, _, got := definitiveStatusDeployment(isvc, test.component)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestStatusExactComponentReplicaRejectsDuplicates(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	ir := commandIR(isvc)
	found := exactComponentReplica(instancecollection.Result{Items: []omev1beta1.InferenceReplica{*ir}}, omev1beta1.EngineComponent)
	require.NotNil(t, found)
	assert.Equal(t, ir.Name, found.Name)
	duplicate := ir.DeepCopy()
	duplicate.Name = "chat-engine-duplicate"
	assert.Nil(t, exactComponentReplica(instancecollection.Result{Items: []omev1beta1.InferenceReplica{*ir, *duplicate}}, omev1beta1.EngineComponent))
	assert.Nil(t, exactComponentReplica(instancecollection.Result{Items: []omev1beta1.InferenceReplica{*ir}}, omev1beta1.RouterComponent))
}

func executeStatus(t *testing.T, f factory.Factory, deps statusDependencies, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	cmd := newStatusCmdWithDependencies(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}}, deps)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func statusCommandDependencies() statusDependencies {
	return statusDependencies{
		clock: commandClock,
		irLimits: instancecollection.Limits{
			Paging: paging.Limits{PageSize: 2, MaxItems: 6, MaxPages: 3, RequestTimeout: time.Second}, MaxStatusRows: 100,
			Details: instancecollection.DetailLimits{MaxConditions: 8, MaxScannedConditions: 16, MaxNodeHints: 8, MaxScannedNodeHints: 16, MaxMigrations: 8, MaxScannedMigrations: 16},
		},
		podLimits:     paging.Limits{PageSize: 4, MaxItems: 8, MaxPages: 2, RequestTimeout: time.Second},
		runtimeLimits: paging.Limits{PageSize: 4, MaxItems: 8, MaxPages: 2, RequestTimeout: time.Second},
		eventLimits: observation.EventLimits{
			Paging: paging.Limits{PageSize: 3, MaxItems: 6, MaxPages: 2, RequestTimeout: time.Second}, MaxTargets: 9, MaxConcurrent: 2,
		},
		projectionLimits: instancestatusprojection.Limits{MaxInstances: 100, MaxPods: 8, MaxContainerStatuses: 16, MaxPodConditions: 16, MaxEvents: 32},
	}
}

func commandStatusPod(isvc *omev1beta1.InferenceService, ir *omev1beta1.InferenceReplica, name string) corev1.Pod {
	controller := true
	canonicalName := query.PodName(isvc.Name, "engine", 0, "default", 0)
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: canonicalName, Namespace: isvc.Namespace, UID: types.UID("uid-" + name),
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc.Name, constants.OMEComponentLabel: "engine",
				query.LabelManagedBy: query.ManagedByOMENative, query.LabelInstanceIdx: "0",
				query.LabelInstanceIncarnation: "1", query.LabelRunner: "default", query.LabelRevisionHash: "a1b2c3", query.LabelPodOrdinal: "0",
			},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: omev1beta1.SchemeGroupVersion.String(), Kind: "InferenceReplica", Name: ir.Name, UID: ir.UID, Controller: &controller}},
		},
		Spec: corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue}, {Type: query.ServingConditionType, Status: corev1.ConditionTrue},
		}},
	}
}

func commandStatusEvent(name, kind, target string, uid types.UID) corev1.Event {
	return corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod", UID: types.UID("uid-" + name)},
		Type:       corev1.EventTypeWarning, Reason: "FailedMount", Count: 1,
		InvolvedObject: corev1.ObjectReference{APIVersion: "v1", Kind: kind, Namespace: "prod", Name: target, UID: uid},
	}
}

func statusRuntimeSpec(mode string) omev1beta1.ServingRuntimeSpec {
	return omev1beta1.ServingRuntimeSpec{EngineConfig: &omev1beta1.EngineSpec{ComponentExtensionSpec: omev1beta1.ComponentExtensionSpec{Annotations: map[string]string{constants.DeploymentMode: mode}}}}
}

func boolPointer(value bool) *bool { return &value }

func statusRuntimeRevision(t *testing.T, namespace, runtimeName string, spec omev1beta1.ServingRuntimeSpec) *appsv1.ControllerRevision {
	t.Helper()
	full, short, err := runtimerevision.Hash(&spec)
	require.NoError(t, err)
	require.Len(t, full, 64)
	raw, err := json.Marshal(&spec)
	require.NoError(t, err)
	return &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
		Name: runtimerevision.Name(runtimerevision.KindServingRuntime, "prod", runtimeName, short), Namespace: namespace, UID: "revision-uid",
		Labels:      map[string]string{constants.RuntimeRevisionOfLabelKey: runtimeName, constants.RuntimeRevisionOfKindLabelKey: "ServingRuntime", constants.RuntimeRevisionOfNamespaceLabelKey: "prod", constants.RuntimeRevisionHashLabelKey: short},
		Annotations: map[string]string{constants.RuntimeRevisionCreatedByKey: constants.RuntimeRevisionCreatedByOMEValue},
	}, Data: runtime.RawExtension{Raw: raw}, Revision: 1}
}

func firstCoreAction(t *testing.T, actions []ktesting.Action, resource string) ktesting.Action {
	t.Helper()
	for _, action := range actions {
		if action.GetResource().Resource == resource {
			return action
		}
	}
	t.Fatalf("no action for %s: %#v", resource, actions)
	return nil
}

type statusFactory struct {
	factory.Factory
	ns         string
	ome        versioned.Interface
	kube       kubernetes.Interface
	omeErr     error
	kubeErr    error
	runtime    ctrlclient.Client
	runtimeErr error
}

func (f statusFactory) Namespace() (string, bool, error)          { return f.ns, false, nil }
func (f statusFactory) OMEClient() (versioned.Interface, error)   { return f.ome, f.omeErr }
func (f statusFactory) KubeClient() (kubernetes.Interface, error) { return f.kube, f.kubeErr }
func (f statusFactory) RuntimeClient() (ctrlclient.Client, error) { return f.runtime, f.runtimeErr }
