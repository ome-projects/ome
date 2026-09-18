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
	"sync/atomic"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/printers"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
	"sigs.k8s.io/ome/pkg/constants"
)

type actionFactory struct {
	factory.Static
	config *rest.Config
}

type actionParserFactory struct{ calls []string }

func (f *actionParserFactory) called(method string) error {
	f.calls = append(f.calls, method)
	return errors.New("unexpected factory acquisition")
}

func (f *actionParserFactory) Namespace() (string, bool, error) {
	return "", false, f.called("Namespace")
}

func (f *actionParserFactory) RESTConfig() (*rest.Config, error) {
	return nil, f.called("RESTConfig")
}

func (f *actionParserFactory) ContextName() (string, error) {
	return "", f.called("ContextName")
}

func (f *actionParserFactory) KubeClient() (kubernetes.Interface, error) {
	return nil, f.called("KubeClient")
}

func (f *actionParserFactory) OMEClient() (versioned.Interface, error) {
	return nil, f.called("OMEClient")
}

func (f *actionParserFactory) RuntimeClient() (ctrlclient.Client, error) {
	return nil, f.called("RuntimeClient")
}

func (f *actionParserFactory) RuntimeClientForAction(context.Context) (ctrlclient.Client, error) {
	return nil, f.called("RuntimeClientForAction")
}

func TestActionParserPrivacyBeforeAnyFactoryAcquisition(t *testing.T) {
	const private = "sk-proj-0123456789abcdefghijklmnopqrstuvwxyz"
	for _, action := range []string{"pause", "resume"} {
		cases := []struct {
			name   string
			suffix []string
		}{
			{"malformed confirmation", []string{"--yes=" + private}},
			{"long control value", []string{"--yes=" + private + "\x1b[2J\n" + strings.Repeat("x", 4096)}},
			{"unknown private flag", []string{"--unknown-" + private + "=value"}},
			{"missing output", []string{"--output"}},
			{"missing dry-run", []string{"--dry-run"}},
			{"missing inherited context", []string{"--context"}},
			{"malformed inherited boolean", []string{"--insecure-skip-tls-verify=" + private}},
		}
		if action == "resume" {
			cases = append(cases, struct {
				name   string
				suffix []string
			}{"malformed discard", []string{"--discard-pending-actions=" + private}})
		}
		for _, tc := range cases {
			t.Run(action+" "+tc.name, func(t *testing.T) {
				var stdout, stderr bytes.Buffer
				f := &actionParserFactory{}
				streams := genericiooptions.IOStreams{Out: &stdout, ErrOut: &stderr}
				cmd := NewCmd(f, streams)
				cmd.SilenceErrors, cmd.SilenceUsage = true, true
				cmd.SetOut(&stdout)
				cmd.SetErr(&stderr)
				cmd.PersistentFlags().Bool("insecure-skip-tls-verify", false, "Inherited parser fixture")
				cmd.PersistentFlags().String("context", "", "Inherited parser fixture")
				cmd.SetArgs(append([]string{action, "example"}, tc.suffix...))
				err := cmd.Execute()
				require.Error(t, err)
				require.Equal(t, 1, exitcode.FromError(err))
				require.Empty(t, stdout.String())
				require.Empty(t, f.calls)
				require.Empty(t, stderr.String())
				if strings.Contains(err.Error(), private) || err.Error() != "invalid rollout action flags; use --help" {
					t.Fatal("parser diagnostic disclosed private input or was not closed/bounded")
				}
			})
		}
	}
}

func TestActionRejectsInvalidNameBeforeFactoryAcquisition(t *testing.T) {
	for _, action := range []string{"pause", "resume", "promote", "rollback"} {
		t.Run(action, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			f := &actionParserFactory{}
			cmd := NewCmd(f, genericiooptions.IOStreams{Out: &stdout, ErrOut: &stderr})
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetOut(&stdout)
			cmd.SetErr(&stderr)
			cmd.SetArgs([]string{action, "bad/name", "--yes", "--dry-run=client"})

			err := cmd.Execute()
			require.ErrorIs(t, err, ErrInvalidInferenceServiceName)
			require.Empty(t, f.calls)
			require.Empty(t, stdout.String())
			require.Empty(t, stderr.String())
		})
	}
}

func newWireFactory(t *testing.T, server *httptest.Server, rt *v1beta1.ServingRuntime) actionFactory {
	t.Helper()
	config := &rest.Config{Host: server.URL}
	ome, err := versioned.NewForConfig(config)
	require.NoError(t, err)
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	return actionFactory{Static: factory.Static{OME: ome, Kube: kubefake.NewClientset(), Runtime: clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(rt).Build(), NS: "prod", Context: "moirai"}, config: config}
}

func TestConditionalTestErrorMatchesActualJSONPatchFailureOnly(t *testing.T) {
	for _, row := range []struct {
		message string
		want    int
	}{
		{"testing value /metadata/resourceVersion failed: test failed", 1},
		{"testing value /metadata/uid failed: test failed", 1},
		{"test operation does not apply: is missing path: /metadata/uid", 1},
		{"jsonpatch test operation failed", 1},
		{"unrelated admission validation failed SECRET_API_VALUE", 1},
		{"jsonpatch test operation is unsupported by admission", 1},
	} {
		err := guardedPatchError(&apierrors.StatusError{ErrStatus: metav1.Status{Code: 422, Reason: metav1.StatusReasonInvalid, Message: row.message}})
		require.Equal(t, row.want, exitcode.FromError(err), row.message)
		require.NotContains(t, err.Error(), "SECRET_API_VALUE")
	}
	canonical := apierrors.NewGenericServerResponse(422, "", schema.GroupResource{}, "", "SECRET_HIDDEN_TEST_FAILURE", 0, false)
	require.Equal(t, 3, exitcode.FromError(guardedPatchError(canonical)))
	for _, change := range []func(*metav1.Status){
		func(s *metav1.Status) { s.Details = nil },
		func(s *metav1.Status) { s.Status = "Success" },
		func(s *metav1.Status) { s.Reason = metav1.StatusReasonForbidden },
		func(s *metav1.Status) { s.Details.Name = "chat" },
		func(s *metav1.Status) { s.Details.Group = "ome.io" },
		func(s *metav1.Status) { s.Details.Kind = "InferenceService" },
		func(s *metav1.Status) { s.Details.UID = "uid" },
		func(s *metav1.Status) { s.Details.RetryAfterSeconds = 1 },
		func(s *metav1.Status) {
			s.Details.Causes = []metav1.StatusCause{{Type: metav1.CauseTypeFieldValueInvalid, Field: "metadata.resourceVersion", Message: "testing value /metadata/uid failed: test failed"}}
		},
	} {
		s := canonical.ErrStatus.DeepCopy()
		change(s)
		require.Equal(t, 1, exitcode.FromError(guardedPatchError(&apierrors.StatusError{ErrStatus: *s})))
	}
}

func TestResumeWirePreservesOtherFieldsAndDiscardsAtomically(t *testing.T) {
	for _, mode := range []string{"client", "server", "none"} {
		for _, discard := range []bool{false, true} {
			t.Run(mode+map[bool]string{false: " idle", true: " discard"}[discard], func(t *testing.T) {
				v, rt, _ := actionFixture()
				v.Status.ObservedGeneration = 0
				v.Annotations[constants.PausedRolloutAnnotation] = "freeze"
				if discard {
					v.Annotations[constants.RolloutPromoteAnnotation] = "cccccccc"
					v.Annotations[constants.RolloutRollbackAnnotation] = ""
				}
				original, err := json.Marshal(v)
				require.NoError(t, err)
				patches, reads := 0, 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if r.Method == "GET" {
						reads++
						require.True(t, strings.HasSuffix(r.URL.Path, "/inferenceservices/chat"), "resume must not infer active IR work")
						_, err = w.Write(original)
						require.NoError(t, err)
						return
					}
					require.Equal(t, "PATCH", r.Method)
					patches++
					if mode == "server" {
						require.Equal(t, "All", r.URL.Query().Get("dryRun"))
					} else {
						require.Empty(t, r.URL.Query().Get("dryRun"))
					}
					body, e := io.ReadAll(r.Body)
					require.NoError(t, e)
					patch, e := jsonpatch.DecodePatch(body)
					require.NoError(t, e)
					require.Len(t, patch, map[bool]int{false: 3, true: 5}[discard])
					next, e := patch.Apply(original)
					require.NoError(t, e)
					var got v1beta1.InferenceService
					require.NoError(t, json.Unmarshal(next, &got))
					delete(v.Annotations, constants.PausedRolloutAnnotation)
					if discard {
						delete(v.Annotations, constants.RolloutPromoteAnnotation)
						delete(v.Annotations, constants.RolloutRollbackAnnotation)
					}
					require.Equal(t, v, &got, "patch must preserve exact other metadata, spec and status")
					_, e = w.Write(next)
					require.NoError(t, e)
				}))
				defer server.Close()
				var out, stderr bytes.Buffer
				cmd := NewCmd(newWireFactory(t, server, rt), genericiooptions.IOStreams{In: bytes.NewBufferString("yes\n"), Out: &out, ErrOut: &stderr})
				args := []string{"resume", "chat", "--yes", "--dry-run=" + mode, "-o", "json"}
				if discard {
					args = append(args, "--discard-pending-actions")
				}
				cmd.SetArgs(args)
				require.NoError(t, cmd.Execute())
				require.Equal(t, 1, reads)
				require.Equal(t, map[bool]int{false: 0, true: 1}[mode != "client"], patches)
				var result reportv1alpha1.ActionResult
				require.NoError(t, json.Unmarshal(out.Bytes(), &result))
				require.Equal(t, "rollout resume", result.Action)
				require.Equal(t, mode == "none", result.Applied)
				require.NotContains(t, out.String()+stderr.String(), "SECRET_PRIVATE_ANNOTATION")
			})
		}
	}
}

func TestSnapshotRacesRejectOnePatchWithoutPartialMutation(t *testing.T) {
	for _, race := range []string{"uid", "mailbox"} {
		t.Run(race, func(t *testing.T) {
			v, rt, ir := actionFixture()
			patches := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == "GET" {
					if strings.HasSuffix(r.URL.Path, "/inferenceservices/chat") {
						require.NoError(t, json.NewEncoder(w).Encode(v))
					} else {
						require.NoError(t, json.NewEncoder(w).Encode(&v1beta1.InferenceReplicaList{Items: []v1beta1.InferenceReplica{*ir}}))
					}
					return
				}
				patches++
				live := v.DeepCopy()
				if race == "uid" {
					live.UID = "uid-recreated"
				} else {
					live.ResourceVersion = "43"
					live.Annotations[constants.RolloutRollbackAnnotation] = "true"
				}
				before, e := json.Marshal(live)
				require.NoError(t, e)
				body, e := io.ReadAll(r.Body)
				require.NoError(t, e)
				patch, e := jsonpatch.DecodePatch(body)
				require.NoError(t, e)
				_, e = patch.Apply(before)
				require.Error(t, e)
				require.NotContains(t, string(before), constants.PausedRolloutAnnotation)
				w.WriteHeader(422)
				status := apierrors.NewGenericServerResponse(422, "", schema.GroupResource{}, "", e.Error(), 0, false).ErrStatus
				status.TypeMeta = metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}
				require.NoError(t, json.NewEncoder(w).Encode(status))
			}))
			defer server.Close()
			var out, stderr bytes.Buffer
			cmd := NewCmd(newWireFactory(t, server, rt), genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
			cmd.SetArgs([]string{"pause", "chat", "--yes"})
			err := cmd.Execute()
			require.Equal(t, 3, exitcode.FromError(err))
			require.Equal(t, 1, patches)
			require.Empty(t, out.String())
		})
	}
}

func (f actionFactory) RESTConfig() (*rest.Config, error) { return f.config, nil }

func actionFixture() (*v1beta1.InferenceService, *v1beta1.ServingRuntime, *v1beta1.InferenceReplica) {
	mode := constants.OMENative
	auto := true
	kind := "ServingRuntime"
	controller := true
	v := &v1beta1.InferenceService{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"}, ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: "uid-chat", ResourceVersion: "42", Generation: 7, Annotations: map[string]string{"private": "SECRET_PRIVATE_ANNOTATION"}}}
	v.Spec.Engine = &v1beta1.EngineSpec{}
	v.Spec.DeploymentMode = &mode
	v.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: "simple", Kind: &kind, AutoSync: &auto}
	v.Status.ObservedGeneration = 7
	rt := &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: "prod", UID: "uid-runtime", ResourceVersion: "99", Generation: 1}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "busybox:1.36"}}}}}
	ir := &v1beta1.InferenceReplica{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica"}, ObjectMeta: metav1.ObjectMeta{Name: "chat-engine", Namespace: "prod", UID: "uid-ir", ResourceVersion: "81", Generation: 2, Labels: map[string]string{constants.InferenceServiceLabel: "chat"}, Annotations: map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: "7"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "chat", UID: v.UID, Controller: &controller}}}, Spec: v1beta1.InferenceReplicaSpec{ParentRef: v1beta1.ParentReference{Name: "chat"}, Component: v1beta1.EngineComponent}, Status: v1beta1.InferenceReplicaStatus{ObservedGeneration: 2, Replicas: 1, CurrentRevision: "chat-engine-aaaaaaaa", UpdateRevision: "chat-engine-bbbbbbbb"}}
	return v, rt, ir
}

func TestPauseWireExactCASDryRunAndStrictOutput(t *testing.T) {
	for _, mode := range []string{"client", "server", "none"} {
		t.Run(mode, func(t *testing.T) {
			v, rt, ir := actionFixture()
			requests := []string{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests = append(requests, r.Method+" "+r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/inferenceservices/chat"):
					require.NoError(t, json.NewEncoder(w).Encode(v))
				case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/inferencereplicas"):
					require.Equal(t, "ome.io/inferenceservice=chat", r.URL.Query().Get("labelSelector"))
					require.Equal(t, "16", r.URL.Query().Get("limit"))
					require.NoError(t, json.NewEncoder(w).Encode(&v1beta1.InferenceReplicaList{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplicaList"}, Items: []v1beta1.InferenceReplica{*ir}}))
				case r.Method == "PATCH":
					require.Equal(t, "/apis/ome.io/v1beta1/namespaces/prod/inferenceservices/chat", r.URL.Path)
					require.Equal(t, "application/json-patch+json", r.Header.Get("Content-Type"))
					body, e := io.ReadAll(r.Body)
					require.NoError(t, e)
					require.JSONEq(t, `[{"op":"test","path":"/metadata/uid","value":"uid-chat"},{"op":"test","path":"/metadata/resourceVersion","value":"42"},{"op":"add","path":"/metadata/annotations/ome.io~1rollout-paused","value":"true"}]`, string(body))
					if mode == "server" {
						require.Equal(t, "All", r.URL.Query().Get("dryRun"))
					} else {
						require.Empty(t, r.URL.Query().Get("dryRun"))
					}
					response := v.DeepCopy()
					response.Annotations[constants.PausedRolloutAnnotation] = "true"
					response.ResourceVersion = "43"
					require.NoError(t, json.NewEncoder(w).Encode(response))
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					http.Error(w, "unexpected", 500)
				}
			}))
			defer server.Close()
			config := &rest.Config{Host: server.URL}
			ome, err := versioned.NewForConfig(config)
			require.NoError(t, err)
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			f := actionFactory{Static: factory.Static{OME: ome, Kube: kubefake.NewClientset(), Runtime: clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(rt).Build(), NS: "prod", Context: "moirai"}, config: config}
			var out, stderr bytes.Buffer
			cmd := newCmdWithClock(f, genericiooptions.IOStreams{In: bytes.NewBufferString("ignored"), Out: &out, ErrOut: &stderr}, reportv1alpha1.ClockFunc(func() time.Time { return time.Date(2026, 9, 15, 21, 0, 0, 0, time.UTC) }))
			cmd.SetArgs([]string{"pause", "chat", "--yes", "--dry-run=" + mode, "-o", "json"})
			require.NoError(t, cmd.ExecuteContext(context.Background()))
			decoder := json.NewDecoder(&out)
			var result reportv1alpha1.ActionResult
			require.NoError(t, decoder.Decode(&result))
			var extra any
			require.ErrorIs(t, decoder.Decode(&extra), io.EOF)
			require.Equal(t, mode != "client", result.Accepted)
			require.Equal(t, mode == "none", result.Applied)
			require.Equal(t, "42", result.Target.ResourceVersion)
			require.Equal(t, "uid-chat", result.Target.UID)
			require.Contains(t, result.FollowUp, "rollout status chat")
			if mode == "client" {
				require.Len(t, requests, 2)
			} else {
				require.Len(t, requests, 3)
			}
			require.Contains(t, stderr.String(), "RestartPolicy repair continues")
			require.NotContains(t, stderr.String(), "SECRET_PRIVATE_ANNOTATION")
			for _, line := range strings.Split(stderr.String(), "\n") {
				require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
			}
		})
	}
}

func TestPauseFlagsAndNonTTYNeverPatch(t *testing.T) {
	for _, args := range [][]string{{"pause", "chat", "--yes", "--dry-run=invalid"}, {"resume", "chat", "--discard-pending-actions"}, {"pause", "chat", "--force"}} {
		var out, stderr bytes.Buffer
		cmd := NewCmd(factory.Static{}, genericiooptions.IOStreams{In: bytes.NewBufferString("yes\n"), Out: &out, ErrOut: &stderr})
		cmd.SetArgs(args)
		require.Error(t, cmd.Execute())
		require.Empty(t, out.String())
	}
}

func TestMutationFailureClassifiesConflictWithoutLeakingAPIMessage(t *testing.T) {
	for _, code := range []int{409, 422, 403} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			v, rt, ir := actionFixture()
			patches := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == "PATCH" {
					patches++
					w.WriteHeader(code)
					reason := "Forbidden"
					if code == 409 {
						reason = "Conflict"
					}
					if code == 422 {
						reason = "Invalid"
					}
					require.NoError(t, json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Code: int32(code), Reason: metav1.StatusReason(reason), Message: "jsonpatch test operation failed SECRET_API_MESSAGE"}))
					return
				}
				if strings.HasSuffix(r.URL.Path, "/inferenceservices/chat") {
					require.NoError(t, json.NewEncoder(w).Encode(v))
				} else {
					require.NoError(t, json.NewEncoder(w).Encode(&v1beta1.InferenceReplicaList{Items: []v1beta1.InferenceReplica{*ir}}))
				}
			}))
			defer server.Close()
			cfg := &rest.Config{Host: server.URL}
			ome, e := versioned.NewForConfig(cfg)
			require.NoError(t, e)
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			f := actionFactory{Static: factory.Static{OME: ome, Kube: kubefake.NewClientset(), Runtime: clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(rt).Build(), NS: "prod", Context: "moirai"}, config: cfg}
			var out, stderr bytes.Buffer
			cmd := NewCmd(f, genericiooptions.IOStreams{In: bytes.NewBuffer(nil), Out: &out, ErrOut: &stderr})
			cmd.SetArgs([]string{"pause", "chat", "--yes"})
			e = cmd.Execute()
			require.Error(t, e)
			require.NotContains(t, e.Error(), "SECRET_API_MESSAGE")
			require.Empty(t, out.String())
			require.Equal(t, 1, patches)
			if code != 409 {
				require.Equal(t, 1, exitcode.FromError(e))
			} else {
				require.Equal(t, 3, exitcode.FromError(e))
			}
		})
	}
}

type privateFailWriter struct{}

func (privateFailWriter) Write([]byte) (int, error) { return 0, errors.New("SECRET_IO_PAYLOAD") }

func TestActionRequiredReadsConfirmationAndIOFailClosed(t *testing.T) {
	for _, scenario := range []string{"non-tty", "preview failure", "forbidden get", "wrong get identity", "unsafe context", "missing context", "missing config", "canceled", "result writer failure"} {
		t.Run(scenario, func(t *testing.T) {
			v, rt, ir := actionFixture()
			patches, reads := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == "PATCH" {
					patches++
					require.NoError(t, json.NewEncoder(w).Encode(v))
					return
				}
				reads++
				if scenario == "forbidden get" {
					w.WriteHeader(403)
					require.NoError(t, json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Code: 403, Reason: metav1.StatusReasonForbidden, Message: "SECRET_REQUIRED_READ"}))
					return
				}
				if strings.HasSuffix(r.URL.Path, "/inferenceservices/chat") {
					if scenario == "wrong get identity" {
						v.Name = "other"
					}
					require.NoError(t, json.NewEncoder(w).Encode(v))
				} else {
					require.NoError(t, json.NewEncoder(w).Encode(&v1beta1.InferenceReplicaList{Items: []v1beta1.InferenceReplica{*ir}}))
				}
			}))
			defer server.Close()
			f := newWireFactory(t, server, rt)
			if scenario == "unsafe context" {
				f.Context = "https://user:SECRET_CONTEXT@example.invalid"
			}
			if scenario == "missing context" {
				f.Context = ""
			}
			if scenario == "missing config" {
				f.config = nil
			}
			var out, stderr bytes.Buffer
			streams := genericiooptions.IOStreams{In: bytes.NewBufferString("yes\n"), Out: &out, ErrOut: &stderr}
			if scenario == "preview failure" {
				streams.ErrOut = privateFailWriter{}
			}
			if scenario == "result writer failure" {
				streams.Out = privateFailWriter{}
			}
			cmd := NewCmd(f, streams)
			args := []string{"pause", "chat", "-o", "json"}
			if scenario != "non-tty" {
				args = append(args, "--yes")
			}
			cmd.SetArgs(args)
			if scenario == "canceled" {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				cmd.SetContext(ctx)
			}
			err := cmd.Execute()
			require.Error(t, err)
			require.NotContains(t, err.Error()+out.String()+stderr.String(), "SECRET_")
			require.Empty(t, out.String())
			if scenario == "result writer failure" {
				require.Equal(t, 1, patches)
				require.Contains(t, err.Error(), "check rollout status")
			} else {
				require.Zero(t, patches)
			}
			if scenario == "unsafe context" || scenario == "missing context" || scenario == "canceled" {
				require.Zero(t, reads)
			}
		})
	}
}

func TestActionUnboundOrOversizedAcceptedResponseNeverClaimsSuccess(t *testing.T) {
	for _, body := range []string{"{PRIVATE_MALFORMED", `{ "apiVersion":"ome.io/v1beta1", "kind":"InferenceService", "metadata":{"name":"chat", "namespace":"prod", "uid":"other", "resourceVersion":"43"}}`, strings.Repeat("x", 1024*1024+1)} {
		v, rt, ir := actionFixture()
		patches := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.Method == "PATCH" {
				patches++
				_, _ = io.WriteString(w, body)
				return
			}
			if strings.HasSuffix(r.URL.Path, "/inferenceservices/chat") {
				require.NoError(t, json.NewEncoder(w).Encode(v))
			} else {
				require.NoError(t, json.NewEncoder(w).Encode(&v1beta1.InferenceReplicaList{Items: []v1beta1.InferenceReplica{*ir}}))
			}
		}))
		var out, stderr bytes.Buffer
		cmd := NewCmd(newWireFactory(t, server, rt), genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
		cmd.SetArgs([]string{"pause", "chat", "--yes", "-o", "json"})
		err := cmd.Execute()
		server.Close()
		require.Error(t, err)
		require.Contains(t, err.Error(), "outcome unknown")
		require.NotContains(t, err.Error(), "PRIVATE_MALFORMED")
		require.Empty(t, out.String())
		require.Equal(t, 1, patches)
	}
}

func TestActionClientOutputAndLeadingDashContextHintParse(t *testing.T) {
	for _, format := range []string{"json", "yaml", "table", "wide"} {
		v, rt, ir := actionFixture()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			w.Header().Set("Content-Type", "application/json")
			if strings.HasSuffix(r.URL.Path, "/inferenceservices/chat") {
				require.NoError(t, json.NewEncoder(w).Encode(v))
			} else {
				require.NoError(t, json.NewEncoder(w).Encode(&v1beta1.InferenceReplicaList{Items: []v1beta1.InferenceReplica{*ir}}))
			}
		}))
		f := newWireFactory(t, server, rt)
		f.Context = "-prod"
		var out, stderr bytes.Buffer
		cmd := NewCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
		cmd.SetArgs([]string{"pause", "chat", "--yes", "--dry-run=client", "-o", format})
		require.NoError(t, cmd.Execute())
		server.Close()
		var result reportv1alpha1.ActionResult
		if format == "yaml" {
			require.NoError(t, yaml.UnmarshalStrict(out.Bytes(), &result))
		} else if format == "json" {
			decoder := json.NewDecoder(&out)
			decoder.DisallowUnknownFields()
			require.NoError(t, decoder.Decode(&result))
			require.ErrorIs(t, decoder.Decode(&result), io.EOF)
		} else {
			require.Contains(t, out.String(), "FIELD")
			for _, line := range strings.Split(out.String(), "\n") {
				require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
			}
			continue
		}
		flags := pflag.NewFlagSet("followup", pflag.ContinueOnError)
		selected := flags.String("context", "", "")
		flags.StringP("namespace", "n", "", "")
		require.NoError(t, flags.Parse(strings.Fields(result.FollowUp)))
		require.Equal(t, "-prod", *selected)
		require.False(t, result.Accepted)
		require.False(t, result.Applied)
	}
}

func TestGuardedPatchPreservesNarrowerConfiguredTimeout(t *testing.T) {
	v, rt, ir := actionFixture()
	var patches atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "PATCH" {
			patches.Add(1)
			select {
			case <-r.Context().Done():
				return
			case <-time.After(300 * time.Millisecond):
			}
			require.NoError(t, json.NewEncoder(w).Encode(v))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/inferenceservices/chat") {
			require.NoError(t, json.NewEncoder(w).Encode(v))
		} else {
			require.NoError(t, json.NewEncoder(w).Encode(&v1beta1.InferenceReplicaList{Items: []v1beta1.InferenceReplica{*ir}}))
		}
	}))
	defer server.Close()
	f := newWireFactory(t, server, rt)
	f.config.Timeout = 20 * time.Millisecond
	var out, stderr bytes.Buffer
	cmd := NewCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
	cmd.SetArgs([]string{"pause", "chat", "--yes", "-o", "json"})
	started := time.Now()
	err := cmd.Execute()
	require.Error(t, err, "the configured request timeout must apply to the guarded PATCH")
	require.Less(t, time.Since(started), 200*time.Millisecond)
	require.Empty(t, out.String())
	require.EqualValues(t, 1, patches.Load())
	require.Equal(t, 20*time.Millisecond, f.config.Timeout)
}

func TestGuardedActionTimeoutHelpIsQualified(t *testing.T) {
	cmd := NewCmd(factory.Static{}, genericiooptions.IOStreams{Out: io.Discard, ErrOut: io.Discard})
	for _, action := range []string{"pause", "resume"} {
		child, _, err := cmd.Find([]string{action})
		require.NoError(t, err)
		require.Contains(t, child.Long, "45-second action context")
		require.Contains(t, child.Long, "shorter --request-timeout")
		require.Contains(t, child.Long, "credential plugins/custom transports")
		require.NotContains(t, child.Long, "total deadline")
	}
}
