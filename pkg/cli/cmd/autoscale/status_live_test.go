package autoscale

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/rest"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

type liveScaleFactory struct {
	factory.Static
	config *rest.Config
}

func (f liveScaleFactory) RESTConfig() (*rest.Config, error) { return f.config, nil }

func liveStatusParent() *ome.InferenceService {
	transition := metav1.NewTime(time.Date(2026, 9, 17, 22, 0, 0, 0, time.UTC))
	return &ome.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: types.UID("parent-uid"), Generation: 3},
		Status: ome.InferenceServiceStatus{Components: map[ome.ComponentType]ome.ComponentStatusSpec{
			ome.EngineComponent: {ScaleTargetRef: &ome.ScaleTargetRef{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "chat-engine"},
				Autoscaler: &ome.ComponentAutoscalerStatus{Class: ome.AutoscalerHPA, ManagedBy: ome.AutoscalerManagedByOME,
					SpecSource: "default", CurrentReplicas: 2, DesiredReplicas: 3,
					Conditions: []metav1.Condition{{Type: "AbleToScale", Status: metav1.ConditionTrue, Reason: "Observed", LastTransitionTime: transition},
						{Type: "ScalingActive", Status: metav1.ConditionTrue, Reason: "Observed", LastTransitionTime: transition}}}},
		}}}
}

func liveStatusIR() *ome.InferenceReplica {
	controller := true
	replicas := int32(3)
	return &ome.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: "chat-engine", Namespace: "prod", UID: types.UID("ir-uid"), ResourceVersion: "9", Generation: 2,
		Annotations:     map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: "3"},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "chat", UID: types.UID("parent-uid"), Controller: &controller}}},
		Spec:   ome.InferenceReplicaSpec{ParentRef: ome.ParentReference{Name: "chat"}, Component: ome.EngineComponent, Replicas: &replicas},
		Status: ome.InferenceReplicaStatus{ObservedGeneration: 2, Replicas: 2}}
}

func TestStatusLiveScaleUsesOnlyExactIRScaleAndRendersCounts(t *testing.T) {
	ir := liveStatusIR()
	ir.TypeMeta = metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica"}
	irBody, err := json.Marshal(ir)
	require.NoError(t, err)
	paths := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine" {
			_, _ = w.Write(irBody)
			return
		}
		_, _ = w.Write([]byte(`{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{"name":"chat-engine","namespace":"prod","uid":"ir-uid","resourceVersion":"9"},"spec":{"replicas":3},"status":{"replicas":2}}`))
	}))
	defer server.Close()
	f := liveScaleFactory{Static: factory.Static{OME: omefake.NewSimpleClientset(liveStatusParent()), NS: "prod"}, config: &rest.Config{Host: server.URL}}
	var output bytes.Buffer
	cmd := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"status", "chat", "--live-scale"})
	require.NoError(t, cmd.Execute())
	t.Logf("synthetic CLI fixture output:\n%s", output.String())
	require.Equal(t, []string{
		"GET /apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine",
		"GET /apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine/scale",
	}, paths)
	for _, want := range []string{"LIVE-EVIDENCE", "Reported", "LIVE-SPEC", "3", "LIVE-CURRENT", "2", "LIVE-COUNT", "Equal"} {
		require.Contains(t, output.String(), want)
	}

	output.Reset()
	cmd = NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"status", "chat", "--live-scale", "-o", "json"})
	require.NoError(t, cmd.Execute())
	var typed reportv1alpha1.AutoscaleStatusReport
	require.NoError(t, json.Unmarshal(output.Bytes(), &typed))
	require.Equal(t, reportv1alpha1.AutoscaleLiveScaleEqual, typed.Content.Components[0].LiveScale.CountComparison)
	require.Equal(t, int32(3), *typed.Content.Components[0].LiveScale.SpecReplicas)

	output.Reset()
	cmd = NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"status", "chat", "--live-scale", "-o", "yaml"})
	require.NoError(t, cmd.Execute())
	require.Contains(t, output.String(), "liveScale:")
	require.Contains(t, output.String(), "countComparison: Equal")

	output.Reset()
	cmd = NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"status", "chat", "--live-scale", "-o", "wide"})
	require.NoError(t, cmd.Execute())
	require.Contains(t, output.String(), "LIVE-EVIDENCE")
	require.Contains(t, output.String(), "LIVE-SPEC")
	require.Contains(t, output.String(), "LIVE-CURRENT")
	require.Contains(t, output.String(), "LIVE-COUNT")
	require.Len(t, paths, 8)
}

func TestStatusLiveScaleDeniedIsTypedAndDoesNotLeakResponse(t *testing.T) {
	ir := liveStatusIR()
	ir.TypeMeta = metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica"}
	irBody, err := json.Marshal(ir)
	require.NoError(t, err)
	paths := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine" {
			_, _ = w.Write(irBody)
			return
		}
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"Status","status":"Failure","message":"private token","reason":"Forbidden","code":403}`))
	}))
	defer server.Close()
	f := liveScaleFactory{Static: factory.Static{OME: omefake.NewSimpleClientset(liveStatusParent()), NS: "prod"}, config: &rest.Config{Host: server.URL}}
	var output bytes.Buffer
	cmd := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &output, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"status", "chat", "--live-scale", "-o", "json"})
	require.NoError(t, cmd.Execute())
	require.Equal(t, []string{
		"/apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine",
		"/apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine/scale",
	}, paths)
	require.NotContains(t, output.String(), "private token")
	var typed reportv1alpha1.AutoscaleStatusReport
	require.NoError(t, json.Unmarshal(output.Bytes(), &typed))
	require.Equal(t, reportv1alpha1.AutoscaleLiveScaleForbidden, typed.Content.Components[0].LiveScale.Evidence)
	require.Nil(t, typed.Content.Components[0].LiveScale.SpecReplicas)
}
