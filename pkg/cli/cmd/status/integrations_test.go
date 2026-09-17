package status

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/acceleratorprojection"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/printers"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/yaml"
)

func TestProjectStatusTrafficSummaryKeepsPolicyFreshness(t *testing.T) {
	v := typedISVC()
	v.Status.Traffic = &v1beta1.TrafficStatus{
		Algorithm: "RoundRobin",
		BackendPolicyResource: &v1beta1.BackendPolicyRef{
			APIVersion: "gateway.envoyproxy.io/v1alpha1", Kind: "BackendTrafficPolicy", Name: "chat",
		},
		Conditions: []metav1.Condition{{Type: v1beta1.TrafficConditionBackendPolicyReady,
			Status: metav1.ConditionUnknown, Reason: v1beta1.TrafficReasonPending,
			Message: "Bearer PRIVATE", ObservedGeneration: 4,
			LastTransitionTime: metav1.NewTime(statusClock.Now().Add(-time.Minute))}},
	}
	before := v.DeepCopy()
	got, err := projectStatus(observedReport(v), statusClock)
	require.NoError(t, err)
	require.Equal(t, before, v)
	require.Equal(t, r.TrafficStatePending, got.Content.Traffic.State)
	require.Equal(t, r.TrafficFreshnessCurrent, got.Content.Traffic.PolicyFreshness)
	require.Equal(t, r.TrafficAlgorithmRoundRobin, got.Content.Traffic.Algorithm)
	require.Equal(t, r.TrafficTranslatorEnvoyGateway, got.Content.Traffic.Translator)
	data, err := json.Marshal(got)
	require.NoError(t, err)
	require.NotContains(t, string(data), "PRIVATE")

	v.Status.Traffic.Conditions[0].ObservedGeneration = 3
	stale, err := projectStatus(observedReport(v), statusClock)
	require.NoError(t, err)
	require.Equal(t, r.TrafficStatePartial, stale.Content.Traffic.State)
	require.Equal(t, r.TrafficFreshnessStale, stale.Content.Traffic.PolicyFreshness)
}

func TestProjectStatusTrafficRejectsOversizedConditionInput(t *testing.T) {
	v := typedISVC()
	v.Status.Traffic = &v1beta1.TrafficStatus{Algorithm: "RoundRobin",
		Conditions: []metav1.Condition{{Type: v1beta1.TrafficConditionBackendPolicyReady,
			Status: metav1.ConditionUnknown, Reason: v1beta1.TrafficReasonPending,
			ObservedGeneration: v.Generation}}}
	for range 64 {
		v.Status.Traffic.Conditions = append(v.Status.Traffic.Conditions,
			metav1.Condition{Type: "Unrelated", Status: metav1.ConditionTrue})
	}
	got, err := projectStatus(observedReport(v), statusClock)
	require.NoError(t, err)
	require.Equal(t, r.TrafficStateUnavailable, got.Content.Traffic.State)
	require.Equal(t, r.EvidenceUnavailable, got.Content.Traffic.Evidence)
}

func TestProjectStatusVirtualAndAutomaticRuntimeStayUnverified(t *testing.T) {
	v := typedISVC()
	v.Spec.Engine = &v1beta1.EngineSpec{}
	v.Annotations = map[string]string{constants.DeploymentMode: string(constants.VirtualDeployment)}
	v.Status.ObservedGeneration = v.Generation
	virtual, err := projectStatus(observedReport(v), statusClock)
	require.NoError(t, err)
	require.Equal(t, r.StatusSummaryUnavailable, virtual.Content.RuntimeSummary.State)
	require.Equal(t, r.StatusReasonVirtualDeployment, virtual.Content.RuntimeSummary.Reason)
	require.Equal(t, r.StatusReasonVirtualDeployment, virtual.Content.Accelerator.Reason)
	require.NotEqual(t, r.StatusSummaryReported, virtual.Content.Accelerator.State)

	v.Annotations = nil
	v.Spec.Model = &v1beta1.ModelRef{Name: "model-a"}
	auto, err := projectStatus(observedReport(v), statusClock)
	require.NoError(t, err)
	require.Equal(t, r.StatusSummaryUnavailable, auto.Content.RuntimeSummary.State)
	require.Equal(t, r.StatusReasonAutoSelectionNotProbed, auto.Content.RuntimeSummary.Reason)
	require.Equal(t, r.StatusReasonAutoSelectionNotProbed, auto.Content.Accelerator.Reason)
}

func TestGatherStatusVirtualSkipsRuntimeAndClassReads(t *testing.T) {
	v := typedISVC()
	v.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: "runtime-a"}
	v.Spec.Engine = &v1beta1.EngineSpec{}
	v.Annotations = map[string]string{constants.DeploymentMode: string(constants.VirtualDeployment)}
	v.Status.ObservedGeneration = v.Generation
	v.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {SelectedAccelerator: &v1beta1.AcceleratorSelection{AcceleratorClass: "gpu-private"}},
	}
	omeClient := omefake.NewSimpleClientset(v)
	snapshot, err := gatherTyped(context.Background(), factory.Static{
		OME: omeClient, Kube: kubefake.NewSimpleClientset(), NS: "prod",
	}, "prod", "chat")
	require.NoError(t, err)
	require.Len(t, omeClient.Actions(), 1)
	require.Equal(t, "inferenceservices", omeClient.Actions()[0].GetResource().Resource)
	got, err := projectStatus(snapshot, statusClock)
	require.NoError(t, err)
	require.Equal(t, r.StatusReasonVirtualDeployment, got.Content.RuntimeSummary.Reason)
	require.Equal(t, r.StatusReasonVirtualDeployment, got.Content.Accelerator.Reason)
	require.NotEqual(t, r.StatusSummaryReported, got.Content.Accelerator.State)
}

func TestStatusIntegratedSummaryJSONYAMLAndWidth(t *testing.T) {
	v := typedISVC()
	v.Status.Traffic = &v1beta1.TrafficStatus{Algorithm: "Default"}
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		out, err := execute(t, factory.Static{OME: omefake.NewSimpleClientset(v), Kube: kubefake.NewSimpleClientset(), NS: "prod"}, "chat", "-o", format)
		require.NoError(t, err)
		if format == "table" || format == "wide" {
			for _, line := range strings.Split(out, "\n") {
				require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
			}
			continue
		}
		var got r.StatusReport
		if format == "json" {
			require.NoError(t, json.Unmarshal([]byte(out), &got))
		} else {
			require.NoError(t, yaml.Unmarshal([]byte(out), &got))
		}
		require.Equal(t, r.TrafficStateUnavailable, got.Content.Traffic.State)
		require.Equal(t, r.StatusSummaryNotConfigured, got.Content.RuntimeSummary.State)
		require.Equal(t, r.StatusSummaryNotConfigured, got.Content.Accelerator.State)
	}
}

func TestStatusAcceleratorClassReadsAreExactBoundedAndOptional(t *testing.T) {
	v := typedISVC()
	v.Status.ObservedGeneration = v.Generation
	v.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent:  {SelectedAccelerator: &v1beta1.AcceleratorSelection{AcceleratorClass: "gpu-a"}},
		v1beta1.DecoderComponent: {SelectedAccelerator: &v1beta1.AcceleratorSelection{AcceleratorClass: "gpu-b"}},
		v1beta1.RouterComponent:  {SelectedAccelerator: &v1beta1.AcceleratorSelection{AcceleratorClass: "gpu-private"}},
	}
	client := omefake.NewSimpleClientset()
	client.PrependReactor("get", "acceleratorclasses", func(action ktesting.Action) (bool, runtime.Object, error) {
		name := action.(ktesting.GetAction).GetName()
		if name == "gpu-b" {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "acceleratorclasses"}, name, errors.New("Bearer PRIVATE"))
		}
		return true, &v1beta1.AcceleratorClass{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name), ResourceVersion: "7", Generation: 1}}, nil
	})
	classes, err := collectStatusAcceleratorClasses(context.Background(), client, v)
	require.NoError(t, err)
	require.Len(t, classes, 2)
	for _, action := range client.Actions() {
		require.Equal(t, "get", action.GetVerb())
		require.Equal(t, "acceleratorclasses", action.GetResource().Resource)
	}
	require.Equal(t, "gpu-a", client.Actions()[0].(ktesting.GetAction).GetName())
	require.Equal(t, "gpu-b", client.Actions()[1].(ktesting.GetAction).GetName())
	want, err := acceleratorprojection.UnavailableAcceleratorClass("gpu-b", r.AcceleratorClassForbidden)
	require.NoError(t, err)
	require.Equal(t, want, classes["gpu-b"])

	v.Status.ObservedGeneration--
	client.ClearActions()
	classes, err = collectStatusAcceleratorClasses(context.Background(), client, v)
	require.NoError(t, err)
	require.Empty(t, classes)
	require.Empty(t, client.Actions())

	v.Status.ObservedGeneration = v.Generation
	ctx, cancel := context.WithCancel(context.Background())
	cancelledClient := omefake.NewSimpleClientset()
	cancelledClient.PrependReactor("get", "acceleratorclasses", func(action ktesting.Action) (bool, runtime.Object, error) {
		cancel()
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "acceleratorclasses"},
			action.(ktesting.GetAction).GetName(), errors.New("Bearer PRIVATE"))
	})
	_, err = collectStatusAcceleratorClasses(ctx, cancelledClient, v)
	require.ErrorIs(t, err, errStatusCancelled)
}

type noRuntimeListClient struct {
	ctrlclient.Client
	lists int
}

func (c *noRuntimeListClient) List(ctx context.Context, list ctrlclient.ObjectList, opts ...ctrlclient.ListOption) error {
	c.lists++
	return errors.New("unexpected broad runtime LIST")
}

type modelReadTrackingClient struct {
	ctrlclient.Client
	modelReads []ctrlclient.ObjectKey
	denyModel  bool
}

func (c *modelReadTrackingClient) Get(ctx context.Context, key ctrlclient.ObjectKey, object ctrlclient.Object, opts ...ctrlclient.GetOption) error {
	switch object.(type) {
	case *v1beta1.BaseModel, *v1beta1.ClusterBaseModel:
		c.modelReads = append(c.modelReads, key)
		if c.denyModel {
			return errors.New("Bearer PRIVATE")
		}
	}
	return c.Client.Get(ctx, key, object, opts...)
}

func TestGatherStatusNamedRuntimeWithModelUsesBoundedModelReads(t *testing.T) {
	v := typedISVC()
	v.Spec.Engine = &v1beta1.EngineSpec{}
	v.Spec.Model = &v1beta1.ModelRef{Name: "model-a"}
	kind := runtimeselector.KindClusterServingRuntime
	v.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: "runtime-a", Kind: &kind}
	v.Status.ObservedGeneration = v.Generation
	runtimeObject := &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{
		Name: "runtime-a", UID: "runtime-uid", ResourceVersion: "9", Generation: 2,
	}, Spec: v1beta1.ServingRuntimeSpec{
		EngineConfig: &v1beta1.EngineSpec{},
		SupportedModelFormats: []v1beta1.SupportedModelFormat{{
			ModelFormat: &v1beta1.ModelFormat{Name: "pytorch", Weight: 10},
		}},
	}}
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{
		Name: "model-a", Namespace: "prod", UID: "model-uid", ResourceVersion: "8", Generation: 2,
	}, Spec: v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "pytorch"}}}
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	boundedClient := &noRuntimeListClient{Client: ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(model, runtimeObject).Build()}
	runtimeClient := &modelReadTrackingClient{Client: boundedClient}
	omeClient := omefake.NewSimpleClientset(v)
	f := factory.Static{
		OME: omeClient, Kube: kubefake.NewSimpleClientset(), Runtime: runtimeClient, NS: "prod",
	}
	snapshot, err := gatherTyped(context.Background(), f, "prod", "chat")
	require.NoError(t, err)
	require.Equal(t, []ctrlclient.ObjectKey{{Namespace: "prod", Name: "model-a"}}, runtimeClient.modelReads)
	require.Zero(t, boundedClient.lists, "explicit runtime must not use broad model/runtime LISTs")
	projected, err := projectStatus(snapshot, statusClock)
	require.NoError(t, err)
	require.Equal(t, r.ConfigurationStateAvailable, projected.Content.RuntimeSummary.ActiveState)
	require.Equal(t, "model-a", projected.Content.Model, "parent model reference is still displayed")

	runtimeClient.denyModel = true
	runtimeClient.modelReads = nil
	deniedSnapshot, err := gatherTyped(context.Background(), f, "prod", "chat")
	require.NoError(t, err, "optional model-read failure must not hide primary status")
	require.Equal(t, []ctrlclient.ObjectKey{{Namespace: "prod", Name: "model-a"}}, runtimeClient.modelReads)
	denied, err := projectStatus(deniedSnapshot, statusClock)
	require.NoError(t, err)
	require.Equal(t, r.StatusSummaryPartial, denied.Content.RuntimeSummary.State)
	require.Equal(t, r.ConfigurationStateUnavailable, denied.Content.RuntimeSummary.ActiveState)
	require.Empty(t, denied.Content.RuntimeSummary.ActiveName)
	require.Equal(t, r.StatusSummaryPartial, denied.Content.Accelerator.State)
	data, err := json.Marshal(denied)
	require.NoError(t, err)
	require.NotContains(t, string(data), "PRIVATE")

	runtimeClient.denyModel = false
	runtimeClient.modelReads = nil
	require.NoError(t, boundedClient.Client.Delete(t.Context(), model))
	require.NoError(t, boundedClient.Client.Create(t.Context(), &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model-a", UID: "cluster-model-uid", Generation: 2},
		Spec:       v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "pytorch"}},
	}))
	clusterSnapshot, err := gatherTyped(context.Background(), f, "prod", "chat")
	require.NoError(t, err)
	require.Equal(t, []ctrlclient.ObjectKey{{Namespace: "prod", Name: "model-a"}, {Name: "model-a"}}, runtimeClient.modelReads,
		"model fallback must use at most two exact GETs")
	require.Zero(t, boundedClient.lists)
	clusterProjected, err := projectStatus(clusterSnapshot, statusClock)
	require.NoError(t, err)
	require.Equal(t, r.ConfigurationStateAvailable, clusterProjected.Content.RuntimeSummary.ActiveState)
}

func TestGatherStatusSharesExplicitRuntimeEvidenceWithoutBroadLists(t *testing.T) {
	v := typedISVC()
	v.Spec.Engine = &v1beta1.EngineSpec{}
	kind := runtimeselector.KindClusterServingRuntime
	v.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: "runtime-a", Kind: &kind}
	v.Status.ObservedGeneration = v.Generation
	v.Status.Traffic = &v1beta1.TrafficStatus{Algorithm: "RoundRobin",
		BackendPolicyResource: &v1beta1.BackendPolicyRef{APIVersion: "gateway.envoyproxy.io/v1alpha1", Kind: "BackendTrafficPolicy", Name: "chat"},
		Conditions: []metav1.Condition{{Type: v1beta1.TrafficConditionBackendPolicyReady,
			Status: metav1.ConditionUnknown, Reason: v1beta1.TrafficReasonPending,
			Message: "Bearer PRIVATE", ObservedGeneration: v.Generation,
			LastTransitionTime: metav1.NewTime(statusClock.Now().Add(-time.Minute))}},
	}
	v.Spec.AcceleratorSelector = &v1beta1.AcceleratorSelector{Policy: v1beta1.BestFitPolicy}
	v.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {SelectedAccelerator: &v1beta1.AcceleratorSelection{
			AcceleratorClass: "gpu-a", ResourceRequests: map[string]string{"example.com/gpu": "1"},
		}},
	}
	runtimeObject := &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{
		Name: "runtime-a", UID: "runtime-uid", ResourceVersion: "9", Generation: 2,
	}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}}}
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	runtimeClient := &noRuntimeListClient{Client: ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(runtimeObject).Build()}
	omeClient := omefake.NewSimpleClientset(v, &v1beta1.AcceleratorClass{ObjectMeta: metav1.ObjectMeta{
		Name: "gpu-a", UID: "class-uid", ResourceVersion: "10", Generation: 1,
	}})
	f := factory.Static{OME: omeClient, Kube: kubefake.NewSimpleClientset(), Runtime: runtimeClient, NS: "prod"}
	snapshot, err := gatherTyped(context.Background(), f, "prod", "chat")
	require.NoError(t, err)
	require.NotNil(t, snapshot.RuntimeState)
	require.NotNil(t, snapshot.AcceleratorBase)
	require.Len(t, snapshot.AcceleratorClasses, 1)
	require.Zero(t, runtimeClient.lists)
	require.Len(t, omeClient.Actions(), 2, "one parent GET and one exact class GET")
	require.Equal(t, "inferenceservices", omeClient.Actions()[0].GetResource().Resource)
	require.Equal(t, "acceleratorclasses", omeClient.Actions()[1].GetResource().Resource)
	projected, err := projectStatus(snapshot, statusClock)
	require.NoError(t, err)
	require.Equal(t, "runtime-a", projected.Content.RuntimeSummary.ActiveName)
	require.Equal(t, r.ConfigurationStateAvailable, projected.Content.RuntimeSummary.ActiveState)
	require.Equal(t, r.StatusFreshnessCurrent, projected.Content.RuntimeSummary.Freshness)
	require.Len(t, projected.Content.Accelerator.Components, 1)
	require.Equal(t, r.AcceleratorClassObserved, projected.Content.Accelerator.Components[0].Class)
	for _, format := range []string{"table", "wide"} {
		out, err := execute(t, f, "chat", "-o", format)
		require.NoError(t, err)
		require.Contains(t, out, "Traffic")
		require.Contains(t, out, "Runtime active")
		require.Contains(t, out, "Accelerator engine")
		require.Contains(t, out, "Unavailable; parent read, no usable scaler status")
		require.NotContains(t, out, "PRIVATE")
		for _, line := range strings.Split(out, "\n") {
			require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
		}
		t.Logf("kubectl ome status chat -n prod -o %s (synthetic fixture):\n%s", format, out)
	}

	omeClient.PrependReactor("get", "acceleratorclasses", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "acceleratorclasses"},
			action.(ktesting.GetAction).GetName(), errors.New("Bearer PRIVATE"))
	})
	omeClient.ClearActions()
	deniedSnapshot, err := gatherTyped(context.Background(), f, "prod", "chat")
	require.NoError(t, err)
	denied, err := projectStatus(deniedSnapshot, statusClock)
	require.NoError(t, err)
	require.Equal(t, r.StatusSummaryPartial, denied.Content.Accelerator.State)
	require.Equal(t, r.AcceleratorClassForbidden, denied.Content.Accelerator.Components[0].Class)
	data, err := json.Marshal(denied)
	require.NoError(t, err)
	require.NotContains(t, string(data), "PRIVATE")

	v.Status.ObservedGeneration--
	require.NoError(t, omeClient.Tracker().Update(v1beta1.SchemeGroupVersion.WithResource("inferenceservices"), v, "prod"))
	omeClient.ClearActions()
	staleSnapshot, err := gatherTyped(context.Background(), f, "prod", "chat")
	require.NoError(t, err)
	require.Len(t, omeClient.Actions(), 1, "stale selection must not trigger class GETs")
	stale, err := projectStatus(staleSnapshot, statusClock)
	require.NoError(t, err)
	require.Equal(t, r.StatusSummaryPartial, stale.Content.RuntimeSummary.State)
	require.Equal(t, r.StatusFreshnessStale, stale.Content.RuntimeSummary.Freshness)
	require.NotEqual(t, r.StatusSummaryReported, stale.Content.Accelerator.State)
}

func TestGatherStatusOptionalRuntimeFailureDoesNotFailPrimary(t *testing.T) {
	v := typedISVC()
	v.Spec.Engine = &v1beta1.EngineSpec{}
	v.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: "runtime-a"}
	omeClient := omefake.NewSimpleClientset(v)
	snapshot, err := gatherTyped(context.Background(), factory.Static{
		OME: omeClient, Kube: kubefake.NewSimpleClientset(), NS: "prod",
	}, "prod", "chat")
	require.NoError(t, err)
	require.Equal(t, r.StatusReasonReadFailed, snapshot.RuntimeReason)
	require.Equal(t, r.StatusReasonReadFailed, snapshot.AcceleratorReason)
	projected, err := projectStatus(snapshot, statusClock)
	require.NoError(t, err)
	require.Equal(t, r.StatusSummaryUnavailable, projected.Content.RuntimeSummary.State)
	require.Equal(t, r.StatusReasonReadFailed, projected.Content.RuntimeSummary.Reason)
	require.Equal(t, r.StatusSummaryUnavailable, projected.Content.Accelerator.State)
	require.Equal(t, r.StatusReasonReadFailed, projected.Content.Accelerator.Reason)
	require.Len(t, omeClient.Actions(), 1, "optional failure must not trigger a duplicate parent read")
}

func TestStatusOMENamespaceFlagIsValidatedBeforeReads(t *testing.T) {
	client := omefake.NewSimpleClientset(typedISVC())
	f := factory.Static{OME: client, Kube: kubefake.NewSimpleClientset(), NS: "prod"}
	_, err := execute(t, f, "chat", "--ome-namespace", "control-plane", "-o", "json")
	require.NoError(t, err)
	require.Len(t, client.Actions(), 1)

	client.ClearActions()
	_, err = execute(t, f, "chat", "--ome-namespace", "invalid/value")
	require.Error(t, err)
	require.Empty(t, client.Actions())
}

func TestStatusOMENamespaceRoutesExactPinGet(t *testing.T) {
	v := typedISVC()
	autoSync := false
	pinName := "pin-a"
	kind := runtimeselector.KindClusterServingRuntime
	v.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: "runtime-a", Kind: &kind,
		AutoSync: &autoSync, Revision: &pinName}
	v.Spec.Engine = &v1beta1.EngineSpec{}
	v.Status.ObservedGeneration = v.Generation
	v.Status.PinnedRevisionName = pinName
	runtimeObject := &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{
		Name: "runtime-a", UID: "runtime-uid", ResourceVersion: "9", Generation: 2,
	}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}}}
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	runtimeClient := &noRuntimeListClient{Client: ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(runtimeObject).Build()}
	kubeClient := kubefake.NewSimpleClientset()
	f := factory.Static{OME: omefake.NewSimpleClientset(v), Kube: kubeClient,
		Runtime: runtimeClient, NS: "prod"}
	_, err := execute(t, f, "chat", "--ome-namespace", "control-plane", "-o", "json")
	require.NoError(t, err)
	require.Zero(t, runtimeClient.lists)
	var pinGets []ktesting.GetAction
	for _, action := range kubeClient.Actions() {
		if action.Matches("get", "controllerrevisions") {
			pinGets = append(pinGets, action.(ktesting.GetAction))
		}
		if action.GetResource().Resource == "controllerrevisions" {
			require.Equal(t, "get", action.GetVerb())
		}
	}
	require.Len(t, pinGets, 1)
	require.Equal(t, "control-plane", pinGets[0].GetNamespace())
	require.Equal(t, pinName, pinGets[0].GetName())
}
