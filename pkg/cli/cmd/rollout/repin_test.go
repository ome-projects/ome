package rollout

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/rest"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/mutate"
	"sigs.k8s.io/ome/pkg/cli/printers"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

var repinCommandNow = time.Date(2026, time.September, 25, 18, 0, 0, 0, time.UTC)

type repinFactory struct {
	factory.Static
	config      *rest.Config
	configErr   error
	configCalls atomic.Int32
}

func (f *repinFactory) RESTConfig() (*rest.Config, error) {
	f.configCalls.Add(1)
	if f.configErr != nil {
		return nil, f.configErr
	}
	return f.config, nil
}

func repinCommandFixture(t *testing.T) *omev1beta1.InferenceService {
	t.Helper()
	mode := constants.OMENative
	oldGroup := omev1beta1.RolloutGroup{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 50},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}
	liveGroup := *oldGroup.DeepCopy()
	liveGroup.Canary.Steps[0].Capacity = intstr.FromString("25%")
	liveGroup.Canary.Steps[0].Traffic = 25
	oldDigest, err := rolloutpolicy.ProgressionDigest(&oldGroup)
	require.NoError(t, err)
	liveDigest, err := rolloutpolicy.ProgressionDigest(&liveGroup)
	require.NoError(t, err)
	service := &omev1beta1.InferenceService{
		TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"},
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: "uid-chat", ResourceVersion: "42", Generation: 7,
			Annotations: map[string]string{"private.example/token": "PRIVATE_TOKEN"}},
		Spec: omev1beta1.InferenceServiceSpec{DeploymentMode: &mode, Engine: &omev1beta1.EngineSpec{}, Rollout: &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{liveGroup}}},
	}
	opened := metav1.NewTime(repinCommandNow.Add(-2 * time.Minute))
	pinned := metav1.NewTime(repinCommandNow.Add(-time.Minute))
	service.Status.Rollout = &omev1beta1.RolloutStatus{
		ActiveRun: &omev1beta1.RolloutRun{
			RunID: "chat-0123456789ab", OpenedAt: opened, PinnedAt: pinned,
			TargetRevisions: []omev1beta1.RolloutRunTarget{{Component: omev1beta1.EngineComponent, Revision: "bbbbbbbb", StableRevision: "aaaaaaaa"}},
			Plan:            omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: oldDigest, Group: oldGroup}}},
		},
		Groups: []omev1beta1.RolloutGroupResolution{{Index: 0, Source: omev1beta1.RolloutPlanSourceInline, ObservedDigest: liveDigest}},
	}
	service.Status.Conditions = duckv1.Conditions{
		{Type: apis.ConditionType(omev1beta1.RolloutPlanReadyCondition), Status: corev1.ConditionTrue, Reason: omev1beta1.RolloutPlanReasonPinned, Message: "run chat-0123456789ab pinned", LastTransitionTime: apis.VolatileTime{Inner: metav1.NewTime(repinCommandNow.Add(-50 * time.Second))}},
		{
			Type:   apis.ConditionType(omev1beta1.RolloutPlanDriftCondition),
			Status: corev1.ConditionTrue,
			Reason: omev1beta1.RolloutPlanDriftReasonSpecNewerThanRun,
			Message: "groups[0]: live render " + liveDigest + " differs from pinned " + oldDigest +
				"; the edit applies at the next run (or via ome.io/rollout-repin)",
			LastTransitionTime: apis.VolatileTime{Inner: metav1.NewTime(repinCommandNow.Add(-30 * time.Second))},
		},
	}
	return service
}

func TestRolloutRepinHelpDeclaresConservativeWireContract(t *testing.T) {
	cmd := newCmdWithClock(&repinFactory{}, genericiooptions.IOStreams{}, reportv1alpha1.SystemClock{})
	repin, _, err := cmd.Find([]string{"repin"})
	require.NoError(t, err)
	require.Equal(t, "repin INFERENCESERVICE", repin.Use)
	for _, flag := range []string{"yes", "dry-run", "output"} {
		require.NotNil(t, repin.Flags().Lookup(flag))
	}
	for _, text := range []string{"one bounded uncached GET", "one PATCH", "never sends the unsafe literal", "empty plans", "topology", "at most one canary", "not controller convergence"} {
		require.Contains(t, repin.Long, text)
	}
	for _, line := range strings.Split(repin.Long, "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80, line)
	}
}

func TestRolloutRepinWireIsOneGETAndAtMostOneExactPatch(t *testing.T) {
	for _, mode := range []string{"client", "server", "none"} {
		t.Run(mode, func(t *testing.T) {
			service := repinCommandFixture(t)
			current := rolloutpolicy.CombinedDigest([]string{service.Status.Rollout.Groups[0].ObservedDigest})
			var requestsMu sync.Mutex
			var requests []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestsMu.Lock()
				requests = append(requests, r.Method+" "+r.URL.RequestURI())
				requestsMu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				switch r.Method {
				case http.MethodGet:
					assert.Equal(t, "/apis/ome.io/v1beta1/namespaces/prod/inferenceservices/chat", r.URL.Path)
					if !assert.NoError(t, json.NewEncoder(w).Encode(service)) {
						return
					}
				case http.MethodPatch:
					assert.Equal(t, "/apis/ome.io/v1beta1/namespaces/prod/inferenceservices/chat", r.URL.Path)
					assert.Equal(t, "application/json-patch+json", r.Header.Get("Content-Type"))
					body, err := io.ReadAll(r.Body)
					if !assert.NoError(t, err) {
						return
					}
					assert.JSONEq(t, `[
                    {"op":"test","path":"/metadata/uid","value":"uid-chat"},
                    {"op":"test","path":"/metadata/resourceVersion","value":"42"},
                    {"op":"add","path":"/metadata/annotations/ome.io~1rollout-repin","value":"`+current+`"}
                  ]`, string(body))
					assert.Equal(t, mode == "server", r.URL.Query().Get("dryRun") == "All")
					response := service.DeepCopy()
					response.ResourceVersion = "43"
					response.Annotations[constants.RolloutRepinAnnotation] = current
					if !assert.NoError(t, json.NewEncoder(w).Encode(response)) {
						return
					}
				default:
					t.Errorf("unexpected request %s", r.Method)
					return
				}
			}))
			defer server.Close()
			config := &rest.Config{Host: server.URL}
			f := &repinFactory{Static: factory.Static{NS: "prod", Context: "moirai"}, config: config}
			var stdout, stderr bytes.Buffer
			cmd := newCmdWithClock(f, genericiooptions.IOStreams{In: bytes.NewBuffer(nil), Out: &stdout, ErrOut: &stderr}, reportv1alpha1.ClockFunc(func() time.Time { return repinCommandNow }))
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs([]string{"repin", "chat", "--yes", "--dry-run=" + mode, "-o=json"})
			require.NoError(t, cmd.ExecuteContext(context.Background()))
			wantRequests := 2
			if mode == "client" {
				wantRequests = 1
			}
			requestsMu.Lock()
			gotRequests := append([]string(nil), requests...)
			requestsMu.Unlock()
			require.Len(t, gotRequests, wantRequests)
			require.Equal(t, int32(1), f.configCalls.Load())
			var result reportv1alpha1.ActionResult
			require.NoError(t, json.Unmarshal(stdout.Bytes(), &result))
			require.Equal(t, "rollout repin", result.Action)
			require.NotNil(t, result.Rollout)
			require.Equal(t, current, result.Rollout.RequestedPlanDigest)
			require.Equal(t, mode != "client", result.Accepted)
			require.Equal(t, mode == "none", result.Applied)
			require.Contains(t, result.FollowUp, "rollout explain chat")
			require.NotContains(t, stdout.String()+stderr.String(), "PRIVATE_TOKEN")
			require.Contains(t, stderr.String(), "ALPHA guarded rollout repin")
			for _, line := range strings.Split(stderr.String(), "\n") {
				require.LessOrEqual(t, printers.CellDisplayWidth(line), 80, line)
			}
		})
	}
}

func TestRolloutRepinRefusesUnsafeEvidenceBeforePreviewOrPatch(t *testing.T) {
	service := repinCommandFixture(t)
	service.Spec.Rollout.Groups = nil
	service.Status.Rollout.Groups = nil
	var gets, patches atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPatch {
			patches.Add(1)
			t.Error("unsafe evidence reached PATCH")
			return
		}
		gets.Add(1)
		if !assert.NoError(t, json.NewEncoder(w).Encode(service)) {
			return
		}
	}))
	defer server.Close()
	f := &repinFactory{Static: factory.Static{NS: "prod", Context: "moirai"}, config: &rest.Config{Host: server.URL}}
	var out, stderr bytes.Buffer
	cmd := newCmdWithClock(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, reportv1alpha1.ClockFunc(func() time.Time { return repinCommandNow }))
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetArgs([]string{"repin", "chat", "--yes"})
	err := cmd.Execute()
	require.Error(t, err)
	require.EqualValues(t, 1, gets.Load())
	require.Zero(t, patches.Load())
	require.Empty(t, out.String())
	require.Empty(t, stderr.String())
}

func TestRolloutRepinRefusesStaleDriftMessageBeforePreviewOrPatch(t *testing.T) {
	service := repinCommandFixture(t)
	service.Status.Conditions[1].Message = "PRIVATE stale drift"
	var gets, patches atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPatch {
			patches.Add(1)
			return
		}
		gets.Add(1)
		if !assert.NoError(t, json.NewEncoder(w).Encode(service)) {
			return
		}
	}))
	defer server.Close()
	f := &repinFactory{
		Static: factory.Static{NS: "prod", Context: "moirai"},
		config: &rest.Config{Host: server.URL},
	}
	var stdout, stderr bytes.Buffer
	cmd := newCmdWithClock(
		f,
		genericiooptions.IOStreams{Out: &stdout, ErrOut: &stderr},
		reportv1alpha1.ClockFunc(func() time.Time { return repinCommandNow }),
	)
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetArgs([]string{"repin", "chat", "--yes"})
	err := cmd.Execute()
	require.ErrorIs(t, err, mutate.ErrRepinEvidence)
	require.EqualValues(t, 1, gets.Load())
	require.Zero(t, patches.Load())
	require.Empty(t, stdout.String()+stderr.String())
	require.NotContains(t, err.Error(), "PRIVATE")
}

func TestRolloutRepinPatchFailuresAreNotRetriedAndAmbiguityIsUnknown(t *testing.T) {
	for _, tc := range []struct {
		name    string
		serve   func(http.ResponseWriter, *http.Request)
		unknown bool
		precond bool
	}{
		{"conflict", func(w http.ResponseWriter, _ *http.Request) {
			err := apierrors.NewConflict(schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}, "chat", errors.New("PRIVATE"))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(&err.ErrStatus)
		}, false, true},
		{"malformed response", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{"metadata":null}`) }, true, false},
		{"wrong accepted annotation", func(w http.ResponseWriter, _ *http.Request) {
			response := &omev1beta1.InferenceService{
				TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"},
				ObjectMeta: metav1.ObjectMeta{
					Name: "chat", Namespace: "prod", UID: "uid-chat", ResourceVersion: "43", Generation: 7,
					Annotations: map[string]string{constants.RolloutRepinAnnotation: "rp1:aaaaaaaaaaaa"},
				},
			}
			_ = json.NewEncoder(w).Encode(response)
		}, true, false},
		{"server error", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(&metav1.Status{Status: metav1.StatusFailure, Code: http.StatusInternalServerError, Reason: metav1.StatusReasonInternalError, Message: "PRIVATE"})
		}, true, false},
		{"redirect", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Location", "https://example.invalid/PRIVATE")
			w.WriteHeader(http.StatusTemporaryRedirect)
			_ = json.NewEncoder(w).Encode(&metav1.Status{
				Status:  metav1.StatusFailure,
				Code:    http.StatusTemporaryRedirect,
				Reason:  metav1.StatusReasonUnknown,
				Message: "PRIVATE",
			})
		}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := repinCommandFixture(t)
			var patches atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodGet {
					_ = json.NewEncoder(w).Encode(service)
					return
				}
				patches.Add(1)
				tc.serve(w, r)
			}))
			defer server.Close()
			f := &repinFactory{Static: factory.Static{NS: "prod", Context: "moirai"}, config: &rest.Config{Host: server.URL}}
			var out, stderr bytes.Buffer
			cmd := newCmdWithClock(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, reportv1alpha1.ClockFunc(func() time.Time { return repinCommandNow }))
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs([]string{"repin", "chat", "--yes", "-o=json"})
			err := cmd.Execute()
			require.Error(t, err)
			require.EqualValues(t, 1, patches.Load())
			require.Empty(t, out.String())
			require.Equal(t, tc.precond, exitcode.FromError(err) == 3)
			require.Equal(t, tc.unknown, strings.Contains(err.Error(), "outcome unknown"))
			require.NotContains(t, err.Error()+stderr.String(), "PRIVATE")
		})
	}
}

func TestRolloutRepinOversizedPatchResponseIsUnknownAndNotRetried(t *testing.T) {
	service := repinCommandFixture(t)
	var patches atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			if !assert.NoError(t, json.NewEncoder(w).Encode(service)) {
				return
			}
			return
		}
		patches.Add(1)
		_, _ = io.WriteString(w, strings.Repeat("x", rolloutRepinResponseLimit+1))
	}))
	defer server.Close()

	f := &repinFactory{
		Static: factory.Static{NS: "prod", Context: "moirai"},
		config: &rest.Config{Host: server.URL},
	}
	var stdout, stderr bytes.Buffer
	cmd := newCmdWithClock(
		f,
		genericiooptions.IOStreams{Out: &stdout, ErrOut: &stderr},
		reportv1alpha1.ClockFunc(func() time.Time { return repinCommandNow }),
	)
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetArgs([]string{"repin", "chat", "--yes", "-o=json"})
	err := cmd.Execute()
	require.EqualError(t, err, "API response exceeds safety bounds; outcome unknown, do not replay, check rollout explain")
	require.EqualValues(t, 1, patches.Load())
	require.Empty(t, stdout.String())
}

func TestRolloutRepinStdoutFailureReportsMutationUncertainty(t *testing.T) {
	for _, mode := range []string{"client", "server", "none"} {
		t.Run(mode, func(t *testing.T) {
			service := repinCommandFixture(t)
			digest := rolloutpolicy.CombinedDigest([]string{service.Status.Rollout.Groups[0].ObservedDigest})
			var patches atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodGet {
					if !assert.NoError(t, json.NewEncoder(w).Encode(service)) {
						return
					}
					return
				}
				patches.Add(1)
				response := service.DeepCopy()
				response.ResourceVersion = "43"
				response.Annotations[constants.RolloutRepinAnnotation] = digest
				if !assert.NoError(t, json.NewEncoder(w).Encode(response)) {
					return
				}
			}))
			defer server.Close()

			f := &repinFactory{
				Static: factory.Static{NS: "prod", Context: "moirai"},
				config: &rest.Config{Host: server.URL},
			}
			var stderr bytes.Buffer
			cmd := newCmdWithClock(
				f,
				genericiooptions.IOStreams{
					Out:    failingWriter{err: errors.New("PRIVATE_WRITER")},
					ErrOut: &stderr,
				},
				reportv1alpha1.ClockFunc(func() time.Time { return repinCommandNow }),
			)
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs([]string{"repin", "chat", "--yes", "--dry-run=" + mode, "-o=json"})
			err := cmd.Execute()
			require.Error(t, err)
			require.NotContains(t, err.Error(), "PRIVATE_WRITER")
			if mode == "client" {
				require.EqualError(t, err, "write rollout repin result failed; no patch sent")
				require.Zero(t, patches.Load())
				return
			}
			require.EqualError(t, err, "write rollout repin result failed after request outcome became unknown; do not replay, check rollout explain")
			require.EqualValues(t, 1, patches.Load())
		})
	}
}

func TestRolloutRepinParserPrivacyPrecedesFactoryAccess(t *testing.T) {
	const private = "sk-proj-0123456789abcdefghijklmnopqrstuvwxyz"
	for _, args := range [][]string{
		{"repin", "chat", "--yes=" + private},
		{"repin", "chat", "--dry-run=" + private},
		{"repin", "chat", "--output=" + private},
		{"repin", "chat", "--unknown-" + private},
	} {
		f := &actionParserFactory{}
		var out, stderr bytes.Buffer
		cmd := NewCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
		cmd.SilenceErrors, cmd.SilenceUsage = true, true
		cmd.SetArgs(args)
		err := cmd.Execute()
		require.Error(t, err)
		require.Equal(t, "invalid rollout repin flags; use --help", err.Error())
		require.Empty(t, f.calls)
		require.Empty(t, out.String()+stderr.String())
	}
}

func TestRolloutRepinStrictlyBindsEveryMutationResponseField(t *testing.T) {
	for _, field := range []string{
		"apiVersion", "kind", "name", "namespace", "uid", "resourceVersion",
		"generation", "annotation", "duplicateAnnotation", "aliasGeneration",
		"aliasAnnotations",
	} {
		t.Run(field, func(t *testing.T) {
			service := repinCommandFixture(t)
			digest := rolloutpolicy.CombinedDigest([]string{service.Status.Rollout.Groups[0].ObservedDigest})
			var patches atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodGet {
					_ = json.NewEncoder(w).Encode(service)
					return
				}
				patches.Add(1)
				response := service.DeepCopy()
				response.ResourceVersion = "43"
				response.Annotations[constants.RolloutRepinAnnotation] = digest
				switch field {
				case "apiVersion":
					response.APIVersion = "other.io/v1"
				case "kind":
					response.Kind = "Other"
				case "name":
					response.Name = "other"
				case "namespace":
					response.Namespace = "other"
				case "uid":
					response.UID = "other"
				case "resourceVersion":
					response.ResourceVersion = "bad value"
				case "generation":
					response.Generation++
				case "annotation":
					delete(response.Annotations, constants.RolloutRepinAnnotation)
				case "duplicateAnnotation":
					_, _ = io.WriteString(w, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"chat","namespace":"prod","uid":"uid-chat","resourceVersion":"43","generation":7,"annotations":{"ome.io/rollout-repin":"`+digest+`","ome.io/rollout-repin":"`+digest+`"}}}`)
					return
				case "aliasGeneration":
					_, _ = io.WriteString(w, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"chat","namespace":"prod","uid":"uid-chat","resourceVersion":"43","generation":8,"Generation":7,"annotations":{"ome.io/rollout-repin":"`+digest+`"}}}`)
					return
				case "aliasAnnotations":
					_, _ = io.WriteString(w, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"chat","namespace":"prod","uid":"uid-chat","resourceVersion":"43","generation":7,"annotations":{},"Annotations":{"ome.io/rollout-repin":"`+digest+`"}}}`)
					return
				}
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			f := &repinFactory{Static: factory.Static{NS: "prod", Context: "moirai"}, config: &rest.Config{Host: server.URL}}
			var out, stderr bytes.Buffer
			cmd := newCmdWithClock(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, reportv1alpha1.ClockFunc(func() time.Time { return repinCommandNow }))
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs([]string{"repin", "chat", "--yes", "-o=json"})
			err := cmd.Execute()
			require.Error(t, err)
			require.Contains(t, err.Error(), "outcome unknown")
			require.EqualValues(t, 1, patches.Load())
			require.Empty(t, out.String())
		})
	}
}

func TestRolloutRepinReadFailuresAreBoundedPrivateAndNeverPreview(t *testing.T) {
	for _, scenario := range []string{
		"oversized", "ambiguous", "case-alias", "missing-gvk", "retry-after",
		"redirect",
	} {
		t.Run(scenario, func(t *testing.T) {
			var reads, destination atomic.Int32
			destinationServer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { destination.Add(1) }))
			defer destinationServer.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				reads.Add(1)
				w.Header().Set("Content-Type", "application/json")
				switch scenario {
				case "oversized":
					_, _ = io.WriteString(w, strings.Repeat("x", rolloutRepinResponseLimit+1))
				case "ambiguous":
					_, _ = io.WriteString(w, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"chat","name":"other","namespace":"prod","uid":"uid-chat","resourceVersion":"42"}}`)
				case "case-alias":
					_, _ = io.WriteString(w, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"chat","namespace":"prod","uid":"uid-chat","resourceVersion":"42"},"status":{},"Status":{"rollout":{}}}`)
				case "missing-gvk":
					_, _ = io.WriteString(w, `{"metadata":{"name":"chat","namespace":"prod","uid":"uid-chat","resourceVersion":"42"}}`)
				case "retry-after":
					w.Header().Set("Retry-After", "0")
					w.WriteHeader(http.StatusTooManyRequests)
					_, _ = io.WriteString(w, `{"apiVersion":"v1","kind":"Status","status":"Failure","message":"PRIVATE","reason":"TooManyRequests","code":429}`)
				case "redirect":
					w.Header().Set("Location", destinationServer.URL+"/PRIVATE")
					w.WriteHeader(http.StatusTemporaryRedirect)
				}
			}))
			defer server.Close()
			f := &repinFactory{Static: factory.Static{NS: "prod", Context: "moirai"}, config: &rest.Config{Host: server.URL}}
			var out, stderr bytes.Buffer
			cmd := newCmdWithClock(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, reportv1alpha1.ClockFunc(func() time.Time { return repinCommandNow }))
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs([]string{"repin", "chat", "--yes"})
			err := cmd.Execute()
			require.Error(t, err)
			require.EqualValues(t, 1, reads.Load(), "GET must not retry")
			require.Zero(t, destination.Load(), "redirect must not be followed")
			require.Empty(t, out.String()+stderr.String())
			require.NotContains(t, err.Error(), "PRIVATE")
		})
	}
}

func TestRolloutRepinConfirmationFailureSendsNoPatch(t *testing.T) {
	service := repinCommandFixture(t)
	var patches atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPatch {
			patches.Add(1)
			return
		}
		_ = json.NewEncoder(w).Encode(service)
	}))
	defer server.Close()
	f := &repinFactory{Static: factory.Static{NS: "prod", Context: "moirai"}, config: &rest.Config{Host: server.URL}}
	var out, stderr bytes.Buffer
	cmd := newCmdWithClock(f, genericiooptions.IOStreams{In: bytes.NewBufferString("yes\n"), Out: &out, ErrOut: &stderr}, reportv1alpha1.ClockFunc(func() time.Time { return repinCommandNow }))
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetArgs([]string{"repin", "chat"})
	err := cmd.Execute()
	require.Error(t, err)
	require.Zero(t, patches.Load())
	require.Empty(t, out.String())
	require.Contains(t, stderr.String(), "ALPHA guarded rollout repin")
}

func TestRolloutRepinSuppressesPrivateWarningsOnReadAndPatch(t *testing.T) {
	service := repinCommandFixture(t)
	digest := rolloutpolicy.CombinedDigest([]string{service.Status.Rollout.Groups[0].ObservedDigest})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Warning", `299 private.example "PRIVATE_WARNING sk-proj-0123456789abcdefghijklmnopqrstuvwxyz"`)
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(service)
			return
		}
		response := service.DeepCopy()
		response.ResourceVersion = "43"
		response.Annotations[constants.RolloutRepinAnnotation] = digest
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	var warnings bytes.Buffer
	config := &rest.Config{Host: server.URL, WarningHandler: rest.NewWarningWriter(&warnings, rest.WarningWriterOptions{})}
	f := &repinFactory{Static: factory.Static{NS: "prod", Context: "moirai"}, config: config}
	var out, stderr bytes.Buffer
	cmd := newCmdWithClock(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, reportv1alpha1.ClockFunc(func() time.Time { return repinCommandNow }))
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetArgs([]string{"repin", "chat", "--yes", "--dry-run=server", "-o=json"})
	require.NoError(t, cmd.Execute())
	require.Empty(t, warnings.String())
	require.NotContains(t, out.String()+stderr.String(), "PRIVATE_WARNING")
}
