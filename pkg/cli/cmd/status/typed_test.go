package status

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"knative.dev/pkg/apis"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/printers"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	clientset "sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/yaml"
)

func TestStatusTypedJSONContract(t *testing.T) {
	v := &ome.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: "private-uid", ResourceVersion: "private-rv"}}
	out, err := execute(t, factory.Static{OME: omefake.NewSimpleClientset(v), Kube: kubefake.NewSimpleClientset(), NS: "prod"}, "chat", "-o", "json")
	require.NoError(t, err)
	var value map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &value))
	require.Equal(t, "StatusReport", value["kind"])
	require.Equal(t, "cli.ome.io/v1alpha1", value["apiVersion"])
	require.NotContains(t, out, "private-uid")
	require.NotContains(t, out, "private-rv")
	require.Contains(t, out, `"NotRecorded"`)
}

func TestTypedStatusDefaultOwnedReadRequestsAndRepresentations(t *testing.T) {
	v := typedISVC()
	v.Spec.Engine = &ome.EngineSpec{}
	v.Status.Conditions = []apis.Condition{condition(corev1.ConditionFalse)}
	p := statusTestPod("chat-engine-0")
	var mu sync.Mutex
	requests := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		mu.Lock()
		requests = append(requests, request.Method+" "+request.URL.Path+"?"+request.URL.RawQuery)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Warning", `299 test "private-server-warning"`)
		switch request.URL.Path {
		case "/apis/ome.io/v1beta1/namespaces/prod/inferenceservices/chat":
			_ = json.NewEncoder(w).Encode(v)
		case "/api/v1/namespaces/prod/pods":
			require.Equal(t, "ome.io/inferenceservice=chat", request.URL.Query().Get("labelSelector"))
			require.Equal(t, "500", request.URL.Query().Get("limit"))
			wrong := *p.DeepCopy()
			wrong.Namespace = "other"
			_ = json.NewEncoder(w).Encode(&corev1.PodList{Items: []corev1.Pod{p, wrong}})
		case "/api/v1/namespaces/prod/events":
			selector := request.URL.Query().Get("fieldSelector")
			require.Contains(t, selector, "involvedObject.uid=")
			require.Contains(t, selector, "type=Warning")
			_ = json.NewEncoder(w).Encode(&corev1.EventList{Items: []corev1.Event{{ObjectMeta: metav1.ObjectMeta{Name: "event", Namespace: "prod"}, Type: corev1.EventTypeWarning, InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "prod", Name: p.Name, UID: p.UID}, Reason: "BackOff", Message: "ordinary operational warning", Count: 1}, {ObjectMeta: metav1.ObjectMeta{Namespace: "other"}, Type: corev1.EventTypeWarning, Reason: "HOSTILE", InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: p.Name, UID: p.UID}}}})
		default:
			t.Errorf("unexpected acquisition %s", request.URL.Path)
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	var reference r.StatusReport
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		flags := genericclioptions.NewConfigFlags(true)
		flags.APIServer = &server.URL
		ns := "prod"
		flags.Namespace = &ns
		var out, stderr bytes.Buffer
		cmd := newCmd(factory.New(flags), genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, statusClock)
		cmd.SetArgs([]string{"chat", "-o", format})
		require.NoError(t, cmd.Execute())
		require.Empty(t, stderr.String())
		require.NotContains(t, out.String(), "private")
		require.NotContains(t, out.String(), "HOSTILE")
		if format == "table" || format == "wide" {
			for _, line := range strings.Split(out.String(), "\n") {
				require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
			}
			require.Contains(t, out.String(), "False / Valid")
		} else {
			var got r.StatusReport
			if format == "json" {
				require.NoError(t, json.Unmarshal(out.Bytes(), &got))
				reference = got
			} else {
				require.NoError(t, yaml.Unmarshal(out.Bytes(), &got))
				require.Equal(t, reference, got)
			}
			require.Equal(t, r.StatusReadyState("False"), got.Content.Ready.Status)
			require.Len(t, got.Content.RecentEvents, 1)
			require.Equal(t, 1, got.Content.Components[0].Pods.Total)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, requests, 16)
	for _, request := range requests {
		require.True(t, strings.HasPrefix(request, "GET "))
	}
}

func TestTypedStatusHelpUsesStreamsAndExplainsEvidence(t *testing.T) {
	var out bytes.Buffer
	cmd := NewCmd(factory.Static{}, genericiooptions.IOStreams{Out: &out, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"--help"})
	require.NoError(t, cmd.Execute())
	for _, word := range []string{"NotRecorded", "Unverifiable", "post-decode", "json", "yaml"} {
		require.Contains(t, out.String(), word)
	}
}

type closedFactory struct {
	factory.Static
	calls int
}

func (f *closedFactory) OMEClient() (clientset.Interface, error) {
	f.calls++
	return nil, errors.New("private credential")
}
func TestTypedStatusClosedParserAndFixedErrors(t *testing.T) {
	for _, args := range [][]string{nil, {"a", "b"}, {"BAD"}, {"chat", "-o", "csv"}, {"chat", "--unknown=private"}} {
		f := &closedFactory{Static: factory.Static{NS: "prod"}}
		_, err := execute(t, f, args...)
		require.Error(t, err)
		require.Zero(t, f.calls)
		require.NotContains(t, err.Error(), "private")
	}
	f := &closedFactory{Static: factory.Static{NS: "BAD"}}
	_, err := execute(t, f, "chat")
	require.Error(t, err)
	require.Zero(t, f.calls)
	f = &closedFactory{Static: factory.Static{NS: "prod"}}
	_, err = execute(t, f, "chat")
	require.EqualError(t, err, "status configuration is unavailable")
}
func TestTypedStatusOptionalPodFailureAndPrimaryBinding(t *testing.T) {
	v := typedISVC()
	kube := kubefake.NewSimpleClientset()
	kube.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, kerrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "chat", errors.New("private"))
	})
	out, err := execute(t, factory.Static{OME: omefake.NewSimpleClientset(v), Kube: kube, NS: "prod"}, "chat", "-o", "json")
	require.NoError(t, err)
	require.Contains(t, out, `"Forbidden"`)
	require.Contains(t, out, `"NotRecorded"`)
	require.NotContains(t, out, "private")
	for _, wrong := range []*ome.InferenceService{func() *ome.InferenceService { v := typedISVC(); v.Namespace = "other"; return v }(), func() *ome.InferenceService { v := typedISVC(); v.UID = ""; return v }(), func() *ome.InferenceService { v := typedISVC(); v.ResourceVersion = ""; return v }()} {
		client := omefake.NewSimpleClientset()
		client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) { return true, wrong, nil })
		kube := kubefake.NewSimpleClientset()
		_, err := execute(t, factory.Static{OME: client, Kube: kube, NS: "prod"}, "chat")
		require.EqualError(t, err, "status source is invalid")
		require.Empty(t, kube.Actions())
	}
}

type statusErrorWriter struct{}

func (statusErrorWriter) Write([]byte) (int, error) { return 0, errors.New("private writer detail") }
func TestTypedStatusCancellationAndOutputFailures(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		f := factory.Static{OME: omefake.NewSimpleClientset(typedISVC()), Kube: kubefake.NewSimpleClientset(), NS: "prod"}
		cmd := NewCmd(f, genericiooptions.IOStreams{Out: statusErrorWriter{}, ErrOut: &bytes.Buffer{}})
		cmd.SetArgs([]string{"chat", "-o", format})
		require.EqualError(t, cmd.Execute(), "status output could not be written")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := factory.Static{OME: omefake.NewSimpleClientset(typedISVC()), Kube: kubefake.NewSimpleClientset(), NS: "prod"}
	cmd := NewCmd(f, genericiooptions.IOStreams{Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"chat"})
	require.EqualError(t, cmd.ExecuteContext(ctx), "status read was cancelled or timed out")
	ctx, cancel = context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	cmd.SetArgs([]string{"chat"})
	require.Error(t, cmd.ExecuteContext(ctx))
}

func TestTypedStatusOptionalCollectionWindowsAndFailures(t *testing.T) {
	for _, sourceErr := range []error{kerrors.NewForbidden(schema.GroupResource{Resource: "events"}, "chat", errors.New("private")), kerrors.NewNotFound(schema.GroupResource{Resource: "events"}, "chat"), context.DeadlineExceeded, errors.New("private")} {
		kube := kubefake.NewSimpleClientset()
		kube.PrependReactor("list", "events", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, sourceErr })
		got, err := gatherTyped(context.Background(), factory.Static{OME: omefake.NewSimpleClientset(typedISVC()), Kube: kube}, "prod", "chat")
		require.NoError(t, err)
		require.Equal(t, r.StatusCollectionState("Unavailable"), got.EventObservation.State)
		require.Equal(t, r.StatusSourceReason(warningEventFailureReason(sourceErr)), got.EventObservation.Reason)
		require.Equal(t, r.StatusCollectionState("Reported"), got.PodObservation.State)
	}
	kube := kubefake.NewSimpleClientset()
	var page atomic.Int32
	kube.PrependReactor("list", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		request := action.(ktesting.ListAction)
		require.Equal(t, "prod", request.GetNamespace())
		if page.Add(1) == 1 {
			p := statusTestPod("chat-engine-0")
			return true, &corev1.PodList{ListMeta: metav1.ListMeta{Continue: "next"}, Items: []corev1.Pod{p}}, nil
		}
		return true, nil, kerrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "chat", errors.New("private"))
	})
	got, err := gatherTyped(context.Background(), factory.Static{OME: omefake.NewSimpleClientset(typedISVC()), Kube: kube}, "prod", "chat")
	require.NoError(t, err)
	require.Equal(t, int32(2), page.Load())
	require.Equal(t, r.StatusCollectionState("Partial"), got.PodObservation.State)
	require.Equal(t, r.StatusSourceReason("Forbidden"), got.PodObservation.Reason)
	require.True(t, got.PodObservation.Truncated)
	require.Len(t, got.Pods[ome.EngineComponent], 1)
	kube = kubefake.NewSimpleClientset()
	pods := make([]corev1.Pod, 21)
	for i := range pods {
		pods[i] = statusTestPod(fmt.Sprintf("chat-engine-%02d", i))
	}
	kube.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) { return true, &corev1.PodList{Items: pods}, nil })
	var eventRequests atomic.Int32
	kube.PrependReactor("list", "events", func(action ktesting.Action) (bool, runtime.Object, error) {
		eventRequests.Add(1)
		selector := action.(ktesting.ListAction).GetListRestrictions().Fields
		name, _ := selector.RequiresExactMatch("involvedObject.name")
		kind, _ := selector.RequiresExactMatch("involvedObject.kind")
		uid, _ := selector.RequiresExactMatch("involvedObject.uid")
		events := make([]corev1.Event, 25)
		for i := range events {
			events[i] = corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-e-%02d", name, i), Namespace: "prod", UID: types.UID(fmt.Sprintf("event-%s-%d", name, i))}, Type: corev1.EventTypeWarning, Reason: "BackOff", InvolvedObject: corev1.ObjectReference{Name: name, Namespace: "prod", Kind: kind, UID: types.UID(uid)}}
		}
		return true, &corev1.EventList{Items: events}, nil
	})
	got, err = gatherTyped(context.Background(), factory.Static{OME: omefake.NewSimpleClientset(typedISVC()), Kube: kube}, "prod", "chat")
	require.NoError(t, err)
	require.Equal(t, int32(16), eventRequests.Load())
	require.Len(t, got.Events, 100)
	require.Equal(t, 6, got.EventObservation.SkippedTargets)
	require.True(t, got.EventObservation.Truncated)
	require.Equal(t, r.StatusCollectionState("Partial"), got.EventObservation.State)
}

func TestTypedStatusPrimaryConfigAndDeadlineFailures(t *testing.T) {
	for _, f := range []factory.Static{{OME: omefake.NewSimpleClientset(typedISVC())}, {Kube: kubefake.NewSimpleClientset()}} {
		_, err := gatherTyped(context.Background(), f, "prod", "chat")
		require.EqualError(t, err, "status configuration is unavailable")
	}
	client := omefake.NewSimpleClientset()
	client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("private URL credential")
	})
	_, err := gatherTyped(context.Background(), factory.Static{OME: client, Kube: kubefake.NewSimpleClientset()}, "prod", "chat")
	require.EqualError(t, err, "status InferenceService could not be read")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		select {
		case <-request.Context().Done():
			return
		case <-time.After(300 * time.Millisecond):
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(typedISVC())
	}))
	defer server.Close()
	for _, narrow := range []bool{false, true} {
		flags := genericclioptions.NewConfigFlags(true)
		flags.APIServer = &server.URL
		ns := "prod"
		flags.Namespace = &ns
		if narrow {
			timeout := "20ms"
			flags.Timeout = &timeout
		}
		var out bytes.Buffer
		cmd := newCmd(factory.New(flags), genericiooptions.IOStreams{Out: &out, ErrOut: &bytes.Buffer{}}, statusClock)
		cmd.SetArgs([]string{"chat"})
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		started := time.Now()
		err := cmd.ExecuteContext(ctx)
		cancel()
		require.Error(t, err)
		require.Less(t, time.Since(started), 200*time.Millisecond)
		require.Empty(t, out.String())
		require.NotContains(t, err.Error(), server.URL)
	}
}

func TestTypedStatusUnsupportedPodComponentsRemainPartial(t *testing.T) {
	p := statusTestPod("chat-unknown-0")
	p.Labels["component"] = "unsupported"
	out, err := execute(t, factory.Static{OME: omefake.NewSimpleClientset(typedISVC()), Kube: kubefake.NewSimpleClientset(&p), NS: "prod"}, "chat", "-o", "json")
	require.NoError(t, err)
	var got r.StatusReport
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	require.Equal(t, r.StatusCollectionState("Partial"), got.Content.Pods.State)
	require.Equal(t, r.StatusSourceReason("MalformedPayload"), got.Content.Pods.Reason)
	require.Contains(t, got.Content.Issues, r.StatusIssueCode("UnsupportedComponent"))
	require.Empty(t, got.Content.Components)
}

func TestTypedStatusJSONYAMLLiteralCanonicalDocument(t *testing.T) {
	const expected = `{"apiVersion":"cli.ome.io/v1alpha1","kind":"StatusReport","metadata":{"namespace":"prod","name":"chat"},"collectedAt":"2026-09-15T12:00:00Z","sources":[{"kind":"InferenceService","namespace":"prod","name":"chat","generation":4,"evidence":"Observed","collectedAt":"2026-09-15T12:00:00Z"}],"content":{"ready":{"status":"NotRecorded","validity":"Unavailable","inspection":{"state":"Complete","total":0,"inspected":0,"warnings":[]}},"generation":4,"observedGeneration":0,"generationFreshness":"Unverifiable","components":[],"pods":{"state":"Reported","observed":0,"truncated":false,"skippedTargets":0},"events":{"state":"Reported","observed":0,"truncated":false,"skippedTargets":0},"recentEvents":[],"rollout":{"summary":{"state":"NotConfigured","reportedState":"NotConfigured","evidence":"Declared","epoch":"NotApplicable","coordinationReady":"NotApplicable"},"issues":[],"warnings":[]},"autoscale":{"summary":{"state":"Unavailable"},"evidence":"Unavailable","components":[],"issues":[]},"issues":[]},"warnings":[]}`
	var reference r.StatusReport
	require.NoError(t, json.Unmarshal([]byte(expected), &reference))
	for _, format := range []string{"json", "yaml"} {
		out, err := execute(t, factory.Static{OME: omefake.NewSimpleClientset(typedISVC()), Kube: kubefake.NewSimpleClientset(), NS: "prod"}, "chat", "-o", format)
		require.NoError(t, err)
		var got r.StatusReport
		if format == "json" {
			var compact bytes.Buffer
			require.NoError(t, json.Compact(&compact, []byte(out)))
			require.Equal(t, expected, compact.String())
			decoder := json.NewDecoder(strings.NewReader(out))
			require.NoError(t, decoder.Decode(&got))
			require.ErrorIs(t, decoder.Decode(&got), io.EOF)
		} else {
			require.NoError(t, yaml.Unmarshal([]byte(out), &got))
			require.NotContains(t, out, "---")
		}
		require.Equal(t, reference, got)
	}
}

func TestTypedStatusAutoscaleFixtureAllFormatsAndReads(t *testing.T) {
	v := typedISVC()
	v.Spec.Engine = &ome.EngineSpec{}
	v.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{
		ome.EngineComponent: {
			Autoscaler: &ome.ComponentAutoscalerStatus{
				Class: ome.AutoscalerHPA, ManagedBy: ome.AutoscalerManagedByOME,
				SpecSource: "isvc", CurrentReplicas: 2, DesiredReplicas: 3,
				Conditions: []metav1.Condition{{Type: "ScalingActive", Status: metav1.ConditionTrue,
					Reason: "Active", Message: "private scaler detail", LastTransitionTime: metav1.NewTime(statusClock.Now())}},
			},
			ScaleTargetRef: &ome.ScaleTargetRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "chat-engine"},
		},
	}
	omeClient := omefake.NewSimpleClientset(v)
	kubeClient := kubefake.NewSimpleClientset()
	f := factory.Static{OME: omeClient, Kube: kubeClient, NS: "prod"}
	var canonical r.StatusReport
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		out, err := execute(t, f, "chat", "-o", format)
		require.NoError(t, err)
		require.NotContains(t, out, "private scaler detail")
		switch format {
		case "table", "wide":
			require.Contains(t, out, "Autoscaling")
			require.Contains(t, out, "Reported / Reported parent status")
			require.Contains(t, out, "2->3 (Reported)")
			for _, line := range strings.Split(out, "\n") {
				require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
			}
			if format == "table" {
				t.Logf("fixture-rendered kubectl ome status chat:\n%s", out)
			}
		case "json":
			require.NoError(t, json.Unmarshal([]byte(out), &canonical))
			require.Equal(t, r.AutoscaleStateReported, canonical.Content.Autoscale.Summary.State)
			require.Equal(t, r.EvidenceReported, canonical.Content.Autoscale.Evidence)
			require.Len(t, canonical.Content.Autoscale.Components, 1)
		case "yaml":
			var document r.StatusReport
			require.NoError(t, yaml.Unmarshal([]byte(out), &document))
			require.Equal(t, canonical, document)
		}
	}
	require.Len(t, omeClient.Actions(), 4)
	for _, action := range omeClient.Actions() {
		require.Equal(t, "get", action.GetVerb())
		require.Equal(t, "inferenceservices", action.GetResource().Resource)
	}
}
