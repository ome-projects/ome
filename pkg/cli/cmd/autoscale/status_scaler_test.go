package autoscale

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/rest"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestStatusLiveScalerOptInHPAUsesExactGETAndDefaultDoesNotRead(t *testing.T) {
	paths := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine" {
			fixture := liveStatusIR()
			fixture.APIVersion, fixture.Kind = "ome.io/v1beta1", "InferenceReplica"
			_ = json.NewEncoder(w).Encode(fixture)
			return
		}
		_, _ = w.Write([]byte(`{"apiVersion":"autoscaling/v2","kind":"HorizontalPodAutoscaler","metadata":{"name":"chat-engine","namespace":"prod","uid":"hpa-uid","generation":4,"ownerReferences":[{"apiVersion":"ome.io/v1beta1","kind":"InferenceReplica","name":"chat-engine","uid":"ir-uid","controller":true}]},"spec":{"scaleTargetRef":{"apiVersion":"ome.io/v1beta1","kind":"InferenceReplica","name":"chat-engine"},"maxReplicas":8},"status":{"observedGeneration":4,"currentReplicas":2,"desiredReplicas":3,"conditions":[{"type":"ScalingActive","status":"True","reason":"private","message":"secret-token"}]}}`))
	}))
	defer server.Close()
	f := liveScaleFactory{Static: factory.Static{OME: omefake.NewSimpleClientset(liveStatusParent(), liveStatusIR()), NS: "prod"}, config: &rest.Config{Host: server.URL}}
	run := func(args ...string) string {
		var out, errOut bytes.Buffer
		cmd := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
		cmd.SetArgs(args)
		require.NoError(t, cmd.Execute(), errOut.String())
		return out.String()
	}
	defaultJSON := run("status", "chat", "-o", "json")
	require.Empty(t, paths)
	require.NotContains(t, defaultJSON, "liveScaler")
	output := run("status", "chat", "--live-scaler", "-o", "json")
	require.Equal(t, []string{"GET /apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine", "GET /apis/autoscaling/v2/namespaces/prod/horizontalpodautoscalers/chat-engine"}, paths)
	var typed reportv1alpha1.AutoscaleStatusReport
	require.NoError(t, json.Unmarshal([]byte(output), &typed))
	require.Equal(t, reportv1alpha1.AutoscaleLiveScalerReported, typed.Content.Components[0].LiveScaler.Evidence)
	require.Equal(t, int32(2), *typed.Content.Components[0].LiveScaler.CurrentReplicas)
	require.NotContains(t, output, "private")
	require.NotContains(t, output, "secret-token")
	table := run("status", "chat", "--live-scaler")
	t.Logf("synthetic CLI fixture output:\n%s", table)
	require.Equal(t, `FIELD              SERVICE    ENGINE        DECODER   ROUTER
STATE              Reported   Reported      -         -
CLASS              -          HPA           -         -
MANAGED-BY         -          ome           -         -
SPEC-SOURCE        -          default       -         -
TARGET-KIND        -          IR            -         -
TARGET-NAME        -          chat-engine   -         -
TARGET-EVIDENCE    -          Reported      -         -
CURRENT            -          2             -         -
DESIRED            -          3             -         -
REPLICA-EVIDENCE   -          Reported      -         -
SCALER-KIND        -          HPA           -         -
SCALER-EVIDENCE    -          Reported      -         -
SCALER-GEN         -          Matched       -         -
SCALER-CURRENT     -          2             -         -
SCALER-DESIRED     -          3             -         -
SCALER-SCALING     -          True          -         -
COND-EVIDENCE      -          Reported      -         -
ABLE-TO-SCALE      -          True          -         -
SCALING-ACTIVE     -          True          -         -
`, table)
	for _, line := range bytes.Split([]byte(table), []byte("\n")) {
		require.LessOrEqual(t, len(line), 80, "table row: %q", line)
	}
}

func TestStatusLiveScalerKEDAUsesOnlySelectedScaledObject(t *testing.T) {
	parent := liveStatusParent()
	status := parent.Status.Components[ome.EngineComponent]
	status.Autoscaler.Class = ome.AutoscalerKEDA
	status.Autoscaler.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Observed", LastTransitionTime: status.Autoscaler.Conditions[0].LastTransitionTime}}
	parent.Status.Components[ome.EngineComponent] = status
	paths := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine" {
			fixture := liveStatusIR()
			fixture.APIVersion, fixture.Kind = "ome.io/v1beta1", "InferenceReplica"
			_ = json.NewEncoder(w).Encode(fixture)
			return
		}
		_, _ = w.Write([]byte(`{"apiVersion":"keda.sh/v1alpha1","kind":"ScaledObject","metadata":{"name":"scaledobject-chat-engine","namespace":"prod","uid":"so-uid","generation":4,"ownerReferences":[{"apiVersion":"ome.io/v1beta1","kind":"InferenceReplica","name":"chat-engine","uid":"ir-uid","controller":true}]},"spec":{"scaleTargetRef":{"apiVersion":"ome.io/v1beta1","kind":"InferenceReplica","name":"chat-engine"}},"status":{"hpaName":"generated-private-hpa","conditions":[{"type":"Ready","status":"True","message":"secret-token"}]}}`))
	}))
	defer server.Close()
	f := liveScaleFactory{Static: factory.Static{OME: omefake.NewSimpleClientset(parent, liveStatusIR()), NS: "prod"}, config: &rest.Config{Host: server.URL}}
	var out bytes.Buffer
	cmd := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"status", "chat", "--live-scaler", "-o", "json"})
	require.NoError(t, cmd.Execute())
	require.Equal(t, []string{"GET /apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine", "GET /apis/keda.sh/v1alpha1/namespaces/prod/scaledobjects/scaledobject-chat-engine"}, paths)
	var typed reportv1alpha1.AutoscaleStatusReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &typed))
	require.Equal(t, reportv1alpha1.AutoscaleLiveScalerReported, typed.Content.Components[0].LiveScaler.Evidence)
	require.Equal(t, reportv1alpha1.AutoscaleScalerGenerationUnproven, typed.Content.Components[0].LiveScaler.GenerationState)
	require.Nil(t, typed.Content.Components[0].LiveScaler.CurrentReplicas)
	require.NotContains(t, out.String(), "generated-private-hpa")
	require.NotContains(t, out.String(), "secret-token")
	out.Reset()
	cmd = NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"status", "chat", "--live-scaler"})
	require.NoError(t, cmd.Execute())
	t.Logf("synthetic KEDA CLI fixture output:\n%s", out.String())
	require.Equal(t, `FIELD              SERVICE    ENGINE        DECODER   ROUTER
STATE              Reported   Reported      -         -
CLASS              -          KEDA          -         -
MANAGED-BY         -          ome           -         -
SPEC-SOURCE        -          default       -         -
TARGET-KIND        -          IR            -         -
TARGET-NAME        -          chat-engine   -         -
TARGET-EVIDENCE    -          Reported      -         -
CURRENT            -          2             -         -
DESIRED            -          3             -         -
REPLICA-EVIDENCE   -          Reported      -         -
SCALER-KIND        -          KEDA          -         -
SCALER-EVIDENCE    -          Reported      -         -
SCALER-GEN         -          Unproven      -         -
SCALER-READY       -          True          -         -
COND-EVIDENCE      -          Reported      -         -
READY              -          True          -         -
`, out.String())
	for _, line := range bytes.Split(out.Bytes(), []byte("\n")) {
		require.LessOrEqual(t, len(line), 80, "table row: %q", line)
	}
}

func TestStatusLiveScalerBoundsAndRedactsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine" {
			fixture := liveStatusIR()
			fixture.APIVersion, fixture.Kind = "ome.io/v1beta1", "InferenceReplica"
			_ = json.NewEncoder(w).Encode(fixture)
			return
		}
		_, _ = w.Write([]byte(strings.Repeat("secret-token", 100000)))
	}))
	defer server.Close()
	f := liveScaleFactory{Static: factory.Static{OME: omefake.NewSimpleClientset(liveStatusParent(), liveStatusIR()), NS: "prod"}, config: &rest.Config{Host: server.URL}}
	var out bytes.Buffer
	cmd := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"status", "chat", "--live-scaler", "-o", "json"})
	require.NoError(t, cmd.Execute())
	var typed reportv1alpha1.AutoscaleStatusReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &typed))
	require.Equal(t, reportv1alpha1.AutoscaleLiveScalerUnavailable, typed.Content.Components[0].LiveScaler.Evidence)
	require.NotContains(t, out.String(), "secret-token")
}

func TestStatusLiveScalerMissingKEDAAPIIsTypedAndRedacted(t *testing.T) {
	parent := liveStatusParent()
	status := parent.Status.Components[ome.EngineComponent]
	status.Autoscaler.Class = ome.AutoscalerKEDA
	parent.Status.Components[ome.EngineComponent] = status
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine" {
			fixture := liveStatusIR()
			fixture.APIVersion, fixture.Kind = "ome.io/v1beta1", "InferenceReplica"
			_ = json.NewEncoder(w).Encode(fixture)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"NotFound","message":"secret-token","code":404}`))
	}))
	defer server.Close()
	f := liveScaleFactory{Static: factory.Static{OME: omefake.NewSimpleClientset(parent), NS: "prod"}, config: &rest.Config{Host: server.URL}}
	var out bytes.Buffer
	cmd := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"status", "chat", "--live-scaler", "-o", "json"})
	require.NoError(t, cmd.Execute())
	var typed reportv1alpha1.AutoscaleStatusReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &typed))
	require.Equal(t, reportv1alpha1.AutoscaleLiveScalerNotFound, typed.Content.Components[0].LiveScaler.Evidence)
	require.NotContains(t, out.String(), "secret-token")
}

func TestStatusLiveScalerAndLiveScaleFlagsCoexist(t *testing.T) {
	paths := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine/scale":
			_, _ = w.Write([]byte(`{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{"name":"chat-engine","namespace":"prod","uid":"ir-uid","resourceVersion":"9"},"spec":{"replicas":3},"status":{"replicas":2}}`))
		case "/apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine":
			fixture := liveStatusIR()
			fixture.APIVersion, fixture.Kind = "ome.io/v1beta1", "InferenceReplica"
			_ = json.NewEncoder(w).Encode(fixture)
		case "/apis/autoscaling/v2/namespaces/prod/horizontalpodautoscalers/chat-engine":
			_, _ = w.Write([]byte(`{"apiVersion":"autoscaling/v2","kind":"HorizontalPodAutoscaler","metadata":{"name":"chat-engine","namespace":"prod","uid":"hpa-uid","generation":4,"ownerReferences":[{"apiVersion":"ome.io/v1beta1","kind":"InferenceReplica","name":"chat-engine","uid":"ir-uid","controller":true}]},"spec":{"scaleTargetRef":{"apiVersion":"ome.io/v1beta1","kind":"InferenceReplica","name":"chat-engine"},"maxReplicas":8},"status":{"observedGeneration":4,"currentReplicas":2,"desiredReplicas":3}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	f := liveScaleFactory{Static: factory.Static{OME: omefake.NewSimpleClientset(liveStatusParent(), liveStatusIR()), NS: "prod"}, config: &rest.Config{Host: server.URL}}
	var out bytes.Buffer
	cmd := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"status", "chat", "--live-scale", "--live-scaler", "-o", "json"})
	require.NoError(t, cmd.Execute())
	require.Equal(t, []string{
		"GET /apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine",
		"GET /apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine/scale",
		"GET /apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine",
		"GET /apis/autoscaling/v2/namespaces/prod/horizontalpodautoscalers/chat-engine",
	}, paths)
	var typed reportv1alpha1.AutoscaleStatusReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &typed))
	require.Equal(t, reportv1alpha1.AutoscaleLiveScaleEqual, typed.Content.Components[0].LiveScale.CountComparison)
	require.Equal(t, reportv1alpha1.AutoscaleLiveScalerReported, typed.Content.Components[0].LiveScaler.Evidence)
}
