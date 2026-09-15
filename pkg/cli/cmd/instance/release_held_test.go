package instance

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
	"sync/atomic"
	"testing"
	"time"

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
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
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

// Without the concrete command, an operator cannot discover or invoke this
// guarded mailbox workflow. This exercises the actual command family.
func TestReleaseHeldInvocation(t *testing.T) {
	var out, errOut bytes.Buffer
	cmd := NewCmd(factory.Static{}, genericiooptions.IOStreams{Out: &out, ErrOut: &errOut})
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetArgs([]string{"release-held", "--help"})
	require.NoError(t, cmd.Execute())
	require.Contains(t, out.String(), "Alpha")
	require.Contains(t, out.String(), "--revision")
	require.Contains(t, out.String(), "--dry-run")
	require.Contains(t, out.String(), "original UID")
	for _, args := range [][]string{
		{"chat"}, {"chat", "--component", "engine"},
		{"chat", "--component", "invalid", "--revision", "aaaaaaaa"},
		{"chat", "--component", "engine", "--revision", "aaaaaaaa", "--yes=sk-proj-0123456789abcdefghijklmnopqrstuvwxyz"},
		{"chat", "--component", "engine", "--revision", "sk-proj-0123456789abcdefghijklmnopqrstuvwxyz"},
	} {
		cmd = NewCmd(factory.Static{}, genericiooptions.IOStreams{Out: &out, ErrOut: &errOut})
		cmd.SilenceErrors, cmd.SilenceUsage = true, true
		cmd.SetArgs(append([]string{"release-held"}, args...))
		err := cmd.Execute()
		require.Error(t, err)
		require.False(t, strings.Contains(err.Error(), "sk-proj-"))
		require.NotContains(t, err.Error(), "static factory")
	}
}

type heldActionFactory struct {
	factory.Static
	config *rest.Config
}

func (f heldActionFactory) RESTConfig() (*rest.Config, error) { return f.config, nil }

type heldFailureFactory struct {
	heldActionFactory
	stage string
}

func (f heldFailureFactory) Namespace() (string, bool, error) {
	if f.stage == "namespace" {
		return "", false, errors.New("PRIVATE_NAMESPACE")
	}
	return f.Static.Namespace()
}
func (f heldFailureFactory) ContextName() (string, error) {
	if f.stage == "context error" {
		return "", errors.New("PRIVATE_CONTEXT")
	}
	if f.stage == "unsafe context" {
		return "PRIVATE\nCONTEXT", nil
	}
	return f.Static.ContextName()
}
func (f heldFailureFactory) RESTConfig() (*rest.Config, error) {
	if f.stage == "config nil" {
		return nil, nil
	}
	if f.stage == "config error" {
		return nil, errors.New("PRIVATE_CONFIG")
	}
	return f.config, nil
}
func (f heldFailureFactory) OMEClient() (versioned.Interface, error) {
	if f.stage == "ome wrapped cancel" {
		return nil, fmt.Errorf("PRIVATE_OME: %w", context.Canceled)
	}
	if f.stage == "ome" {
		return nil, errors.New("PRIVATE_OME")
	}
	return f.Static.OMEClient()
}
func (f heldFailureFactory) KubeClient() (kubernetes.Interface, error) {
	if f.stage == "kube" {
		return nil, errors.New("PRIVATE_KUBE")
	}
	return f.Static.KubeClient()
}
func (f heldFailureFactory) RuntimeClient() (ctrlclient.Client, error) {
	if f.stage == "runtime wrapped deadline" {
		return nil, fmt.Errorf("PRIVATE_RUNTIME: %w", context.DeadlineExceeded)
	}
	if f.stage == "runtime" {
		return nil, errors.New("PRIVATE_RUNTIME")
	}
	return f.Static.RuntimeClient()
}

func TestReleaseHeldRequiredClientsAndNativeSourcesFailClosed(t *testing.T) {
	for _, stage := range []string{"namespace", "context error", "unsafe context", "config nil", "config error", "ome", "kube", "runtime", "ome wrapped cancel", "runtime wrapped deadline", "wrong parent", "oversized parent", "raw deployment", "missing component"} {
		t.Run(stage, func(t *testing.T) {
			h := newHeldWireHarness(t)
			switch stage {
			case "wrong parent":
				h.parent.Name = "other"
			case "oversized parent":
				h.parent.Annotations["private"] = strings.Repeat("PRIVATE", 160000)
			case "raw deployment":
				mode := constants.RawDeployment
				h.parent.Spec.DeploymentMode = &mode
			case "missing component":
				h.parent.Spec.Engine = nil
			}
			var out, stderr bytes.Buffer
			cmd := newReleaseHeldCmd(heldFailureFactory{heldActionFactory: h.f, stage: stage}, genericiooptions.IOStreams{In: bytes.NewBuffer(nil), Out: &out, ErrOut: &stderr}, reportv1alpha1.SystemClock{})
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs([]string{"chat", "--component=engine", "--revision=aaaaaaaa", "--yes"})
			err := cmd.Execute()
			require.Error(t, err)
			require.NotContains(t, err.Error()+stderr.String(), "PRIVATE")
			require.Empty(t, out.String())
			require.Zero(t, h.patches)
		})
	}
}

type heldCommandFallbackClient struct {
	ctrlclient.Client
	forbidName   string
	clusterReads atomic.Int32
}

func (c *heldCommandFallbackClient) Get(ctx context.Context, key ctrlclient.ObjectKey, value ctrlclient.Object, opts ...ctrlclient.GetOption) error {
	if key.Namespace == "" {
		c.clusterReads.Add(1)
	}
	if c.forbidName != "" && key.Namespace == "prod" && key.Name == c.forbidName {
		return apierrors.NewForbidden(schema.GroupResource{Group: "ome.io", Resource: "source"}, "PRIVATE_API_NAME", errors.New("PRIVATE_API_PROSE"))
	}
	return c.Client.Get(ctx, key, value, opts...)
}

func TestReleaseHeldCommandPreservesNormalClusterFallbackWithoutForbiddenBypass(t *testing.T) {
	for _, scenario := range []string{"model", "inheritance"} {
		for _, forbidden := range []bool{false, true} {
			t.Run(fmt.Sprint(scenario, "/forbidden=", forbidden), func(t *testing.T) {
				h := newHeldWireHarness(t)
				missing := ""
				if scenario == "model" {
					h.parent.Spec.Model = &v1beta1.ModelRef{Name: "llama"}
					model := &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "llama", UID: "uid-model", Generation: 1}}
					model.Spec.ModelFormat.Name = "safetensors"
					require.NoError(t, h.f.Runtime.Create(context.Background(), model))
					missing = "llama"
				} else {
					h.runtime.Annotations = map[string]string{constants.RuntimeInheritFromAnnotationKey: "base"}
					require.NoError(t, h.f.Runtime.Update(context.Background(), h.runtime))
					base := &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "base", UID: "uid-base", Generation: 1}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{}}}
					require.NoError(t, h.f.Runtime.Create(context.Background(), base))
					missing = "base"
				}
				trace := &heldCommandFallbackClient{Client: h.f.Runtime}
				if forbidden {
					trace.forbidName = missing
				}
				h.f.Runtime = trace
				out, stderr, err := h.execute(t, "chat", "--component=engine", "--revision=aaaaaaaa", "--yes", "--dry-run=client", "-o=json")
				if forbidden {
					require.Error(t, err)
					require.Empty(t, out)
					require.Zero(t, trace.clusterReads.Load())
					require.NotContains(t, err.Error(), "PRIVATE")
				} else {
					require.NoError(t, err)
					require.NotEmpty(t, out)
					require.Positive(t, trace.clusterReads.Load())
					require.Equal(t, 2, h.parentReads)
				}
				require.Zero(t, h.patches)
				require.NotContains(t, out+stderr, "PRIVATE")
			})
		}
	}
}

func TestReleaseHeldSharedMutationWarningsAndFutureFieldsStayPrivate(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		h := newHeldWireHarness(t)
		var warnings bytes.Buffer
		h.f.config.WarningHandler = rest.NewWarningWriter(&warnings, rest.WarningWriterOptions{})
		h.patchReply = func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Warning", `299 private.example "PRIVATE_TOKEN sk-proj-0123456789abcdefghijklmnopqrstuvwxyz"`)
			if ambiguous {
				_, _ = io.WriteString(w, `{"metadata":null}`)
				return
			}
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"apiVersion": "ome.io/v1beta1", "kind": "InferenceReplica", "metadata": map[string]any{"name": "actual-native-engine", "namespace": "prod", "uid": "uid-ir", "resourceVersion": "82", "futureMetadata": map[string]string{"private": "PRIVATE_METADATA"}}, "futureStatus": map[string]string{"private": "PRIVATE_FUTURE_FIELD"}}))
		}
		out, stderr, err := h.execute(t, "chat", "--component=engine", "--revision=aaaaaaaa", "--yes", "--dry-run=server", "-o=json")
		if ambiguous {
			require.Error(t, err)
			require.Empty(t, out)
		} else {
			require.NoError(t, err)
			require.Contains(t, out, `"accepted": true`)
		}
		require.Equal(t, 1, h.patches)
		require.Empty(t, warnings.String())
		require.NotContains(t, out+stderr, "PRIVATE")
	}
}

func TestReleaseHeldResponseGuardClosed(t *testing.T) {
	target := reportv1alpha1.ActionTarget{Kind: "InferenceReplica", Name: "actual-native-engine", Namespace: "prod", UID: "uid-ir"}
	for _, body := range []string{strings.Repeat("x", 1024*1024+1), `null`, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceReplica","metadata":{"name":"actual-native-engine","namespace":"prod","uid":"uid-ir","resourceVersion":"PRIVATE\\nRV"}}`} {
		require.False(t, heldReleaseResponseMatches([]byte(body), target))
	}
}

func TestReleaseHeldDoesNotMutateCallerExecConfiguration(t *testing.T) {
	h := newHeldWireHarness(t)
	original := &runtime.Unknown{Raw: []byte(`{"private":"PRIVATE_EXEC_CONFIG"}`)}
	provider := &clientcmdapi.ExecConfig{Command: "synthetic-never-executed", APIVersion: "client.authentication.k8s.io/v1beta1", InteractiveMode: clientcmdapi.NeverExecInteractiveMode, Config: original}
	h.f.config.ExecProvider = provider
	h.f.config.Timeout = 20 * time.Second
	out, stderr, err := h.execute(t, "chat", "--component=engine", "--revision=aaaaaaaa", "--yes", "--dry-run=client")
	require.NoError(t, err)
	require.NotContains(t, out+stderr, "PRIVATE")
	require.Zero(t, h.patches)
	require.Same(t, provider, h.f.config.ExecProvider)
	require.Same(t, original, provider.Config)
	require.Equal(t, 20*time.Second, h.f.config.Timeout)
}

type heldParserFactory struct {
	factory.Static
	calls int
}

func (f *heldParserFactory) Namespace() (string, bool, error) {
	f.calls++
	return "", false, errors.New("PRIVATE_FACTORY_ACQUIRED")
}

func TestReleaseHeldClosedParserNeverAcquiresFactory(t *testing.T) {
	base := []string{"chat", "--component=engine", "--revision=aaaaaaaa", "--yes"}
	for _, tail := range [][]string{{"-o", ""}, {"--output="}, {"-o=invalid"}, {"--dry-run="}, {"--dry-run=true"}, {"--component="}, {"--revision="}, {"--force"}, {"--retry"}, {"--ome-namespace=PRIVATE\n"}, {"second"}} {
		f := &heldParserFactory{}
		var out, stderr bytes.Buffer
		cmd := newReleaseHeldCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, reportv1alpha1.SystemClock{})
		cmd.SilenceErrors, cmd.SilenceUsage = true, true
		cmd.SetArgs(append(append([]string{}, base...), tail...))
		err := cmd.Execute()
		require.Error(t, err)
		require.Zero(t, f.calls)
		require.Empty(t, out.String())
		require.NotContains(t, err.Error(), "PRIVATE")
	}
}

func heldActionFixture() (*v1beta1.InferenceService, *v1beta1.ServingRuntime, *v1beta1.InferenceReplica) {
	mode, kind, auto, controller := constants.OMENative, "ServingRuntime", true, true
	v := &v1beta1.InferenceService{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"}, ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: "uid-chat", ResourceVersion: "42", Generation: 7, Annotations: map[string]string{"private": "PRIVATE_PARENT_ANNOTATION", constants.PausedRolloutAnnotation: "true"}}}
	v.Spec.Engine = &v1beta1.EngineSpec{}
	v.Spec.DeploymentMode = &mode
	v.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: "simple", Kind: &kind, AutoSync: &auto}
	rt := &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: "prod", UID: "uid-runtime", ResourceVersion: "99", Generation: 1}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "busybox:1.36", Env: []corev1.EnvVar{{Name: "TOKEN", Value: "PRIVATE_RUNTIME_ENV"}}}}}}}
	ir := &v1beta1.InferenceReplica{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica"}, ObjectMeta: metav1.ObjectMeta{Name: "actual-native-engine", Namespace: "prod", UID: "uid-ir", ResourceVersion: "81", Generation: 2, Labels: map[string]string{constants.InferenceServiceLabel: "chat"}, Annotations: map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: "7", constants.InferenceReplicaControllerWriteAnnotationKey: "true", "private": "PRIVATE_IR_ANNOTATION"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "chat", UID: v.UID, Controller: &controller}}}, Spec: v1beta1.InferenceReplicaSpec{ParentRef: v1beta1.ParentReference{Name: "chat"}, Component: v1beta1.EngineComponent}, Status: v1beta1.InferenceReplicaStatus{ObservedGeneration: 2, Replicas: 1, CurrentRevision: "chat-engine-aaaaaaaa", UpdateRevision: "chat-engine-bbbbbbbb", RetryBlocks: []v1beta1.RetryBlock{{TargetRevision: "chat-engine-aaaaaaaa", State: v1beta1.RetryBlockHeld, AttemptsStarted: 3, Reason: "PRIVATE_BLOCK_REASON"}}}}
	return v, rt, ir
}

type heldWireHarness struct {
	parent                                  *v1beta1.InferenceService
	runtime                                 *v1beta1.ServingRuntime
	ir                                      *v1beta1.InferenceReplica
	siblings                                []v1beta1.InferenceReplica
	parentReads, lists, exactReads, patches int
	patchBody                               []byte
	patchQuery                              string
	beforeParent                            func(int)
	beforeList                              func(int)
	beforeExact                             func(int)
	patchReply                              func(http.ResponseWriter, *http.Request)
	f                                       heldActionFactory
}

func newHeldWireHarness(t *testing.T) *heldWireHarness {
	t.Helper()
	v, rt, ir := heldActionFixture()
	h := &heldWireHarness{parent: v, runtime: rt, ir: ir}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/inferenceservices/chat"):
			h.parentReads++
			if h.beforeParent != nil {
				h.beforeParent(h.parentReads)
			}
			require.NoError(t, json.NewEncoder(w).Encode(h.parent))
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/inferencereplicas"):
			h.lists++
			if h.beforeList != nil {
				h.beforeList(h.lists)
			}
			require.Equal(t, "ome.io/inferenceservice=chat", r.URL.Query().Get("labelSelector"))
			require.Equal(t, "16", r.URL.Query().Get("limit"))
			items := append([]v1beta1.InferenceReplica{*h.ir}, h.siblings...)
			require.NoError(t, json.NewEncoder(w).Encode(&v1beta1.InferenceReplicaList{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplicaList"}, Items: items}))
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/inferencereplicas/actual-native-engine"):
			h.exactReads++
			if h.beforeExact != nil {
				h.beforeExact(h.exactReads)
			}
			require.NoError(t, json.NewEncoder(w).Encode(h.ir))
		case r.Method == "PATCH":
			h.patches++
			require.Equal(t, "/apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/actual-native-engine", r.URL.Path)
			require.Equal(t, "application/json-patch+json", r.Header.Get("Content-Type"))
			var err error
			h.patchBody, err = io.ReadAll(r.Body)
			require.NoError(t, err)
			h.patchQuery = r.URL.Query().Get("dryRun")
			if h.patchReply != nil {
				h.patchReply(w, r)
				return
			}
			response := h.ir.DeepCopy()
			response.ResourceVersion = "82"
			require.NoError(t, json.NewEncoder(w).Encode(response))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected", 500)
		}
	}))
	t.Cleanup(server.Close)
	cfg := &rest.Config{Host: server.URL}
	ome, err := versioned.NewForConfig(cfg)
	require.NoError(t, err)
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	h.f = heldActionFactory{Static: factory.Static{OME: ome, Kube: kubefake.NewClientset(), Runtime: clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(rt).Build(), NS: "prod", Context: "synthetic"}, config: cfg}
	return h
}

func (h *heldWireHarness) execute(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	var out, stderr bytes.Buffer
	cmd := newReleaseHeldCmd(h.f, genericiooptions.IOStreams{In: bytes.NewBuffer(nil), Out: &out, ErrOut: &stderr}, reportv1alpha1.ClockFunc(func() time.Time { return time.Date(2026, 9, 15, 21, 0, 0, 0, time.UTC) }))
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(context.Background())
	return out.String(), stderr.String(), err
}

func TestReleaseHeldExactWireAllDryRunModesAndFormats(t *testing.T) {
	for _, mode := range []string{"client", "server", "none"} {
		for _, format := range []string{"table", "wide", "json", "yaml"} {
			t.Run(mode+"/"+format, func(t *testing.T) {
				h := newHeldWireHarness(t)
				out, stderr, err := h.execute(t, "chat", "--component=engine", "--revision=aaaaaaaa", "--yes", "--dry-run="+mode, "-o", format)
				require.NoError(t, err)
				require.Equal(t, 2, h.parentReads)
				require.Equal(t, 2, h.lists)
				require.Equal(t, 2, h.exactReads)
				require.NotContains(t, out+stderr, "PRIVATE_")
				require.Contains(t, stderr, "Unverifiable")
				require.Contains(t, stderr, "existing controller-write=true preserved")
				for _, line := range strings.Split(stderr+out, "\n") {
					if format == "table" || format == "wide" {
						require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
					}
				}
				if mode == "client" {
					require.Zero(t, h.patches)
				} else {
					require.Equal(t, 1, h.patches)
					require.JSONEq(t, `[{"op":"test","path":"/metadata/uid","value":"uid-ir"},{"op":"test","path":"/metadata/resourceVersion","value":"81"},{"op":"add","path":"/metadata/annotations/ome.io~1release-held-revision","value":"chat-engine-aaaaaaaa"}]`, string(h.patchBody))
					if mode == "server" {
						require.Equal(t, "All", h.patchQuery)
					} else {
						require.Empty(t, h.patchQuery)
					}
				}
				if format == "json" || format == "yaml" {
					if format == "yaml" {
						body, e := yaml.YAMLToJSON([]byte(out))
						require.NoError(t, e)
						out = string(body)
					}
					decoder := json.NewDecoder(strings.NewReader(out))
					var result reportv1alpha1.ActionResult
					require.NoError(t, decoder.Decode(&result))
					var extra any
					require.ErrorIs(t, decoder.Decode(&extra), io.EOF)
					require.Equal(t, "instance release-held", result.Action)
					require.Equal(t, "InferenceReplica", result.Target.Kind)
					require.Equal(t, "actual-native-engine", result.Target.Name)
					require.Equal(t, "uid-ir", result.Target.UID)
					require.Equal(t, "81", result.Target.ResourceVersion)
					require.Equal(t, "aaaaaaaa", result.RevisionHash)
					require.Equal(t, mode != "client", result.Accepted)
					require.Equal(t, mode == "none", result.Applied)
					require.Equal(t, "kubectl ome instance retry-blocks chat --component=engine -n prod --context=synthetic", result.FollowUp)
					require.NotContains(t, result.Message, "released")
				}
			})
		}
	}
}

func TestReleaseHeldOneFiniteRefreshRefusesSourceChanges(t *testing.T) {
	for _, scenario := range []string{"parent RV", "parent UID", "parent generation", "selected RV", "mailbox empty", "state", "counter", "missing blocks", "exact list race", "runtime", "sibling added", "sibling removed", "sibling RV", "sibling deleting", "duplicate component"} {
		t.Run(scenario, func(t *testing.T) {
			h := newHeldWireHarness(t)
			sibling := h.ir.DeepCopy()
			sibling.Name = "native-decoder"
			sibling.UID = "uid-decoder"
			sibling.Spec.Component = v1beta1.DecoderComponent
			sibling.Status.RetryBlocks = nil
			if scenario == "sibling removed" || scenario == "sibling RV" || scenario == "sibling deleting" {
				h.siblings = []v1beta1.InferenceReplica{*sibling}
			}
			h.beforeParent = func(n int) {
				if n != 2 {
					return
				}
				switch scenario {
				case "parent RV":
					h.parent.ResourceVersion = "43"
				case "parent UID":
					h.parent.UID = "new-uid"
				case "parent generation":
					h.parent.Generation = 8
				case "runtime":
					rt := h.runtime.DeepCopy()
					rt.Spec.EngineConfig.Runner.Image = "busybox:1.35"
					require.NoError(t, h.f.Runtime.Update(context.Background(), rt))
				}
			}
			h.beforeList = func(n int) {
				if n != 2 {
					return
				}
				switch scenario {
				case "selected RV":
					h.ir.ResourceVersion = "82"
				case "mailbox empty":
					h.ir.Annotations[constants.ReleaseHeldRevisionAnnotationKey] = ""
				case "state":
					h.ir.Status.RetryBlocks[0].State = v1beta1.RetryBlockRetryInProgress
				case "counter":
					h.ir.Status.RetryBlocks[0].AttemptsStarted++
				case "missing blocks":
					h.ir.Status.RetryBlocks = nil
				case "sibling added":
					h.siblings = []v1beta1.InferenceReplica{*sibling}
				case "sibling removed":
					h.siblings = nil
				case "sibling RV":
					h.siblings[0].ResourceVersion = "82"
				case "sibling deleting":
					tm := metav1.Now()
					h.siblings[0].DeletionTimestamp = &tm
				case "duplicate component":
					sibling.Spec.Component = v1beta1.EngineComponent
					h.siblings = []v1beta1.InferenceReplica{*sibling}
				}
			}
			h.beforeExact = func(n int) {
				if n == 2 && scenario == "exact list race" {
					h.ir.ResourceVersion = "83"
				}
			}
			out, _, err := h.execute(t, "chat", "--component=engine", "--revision=aaaaaaaa", "--yes")
			require.Error(t, err)
			require.Equal(t, 3, exitcode.FromError(err))
			require.Empty(t, out)
			require.Zero(t, h.patches)
			require.Equal(t, 2, h.parentReads)
			require.LessOrEqual(t, h.lists, 2)
			require.LessOrEqual(t, h.exactReads, 2)
		})
	}
}

func TestReleaseHeldPendingAndNonTTYRefuseWithoutPatch(t *testing.T) {
	for _, scenario := range []string{"nonTTY", "pending empty", "pending false", "pending same", "missing provenance", "unknown block"} {
		t.Run(scenario, func(t *testing.T) {
			h := newHeldWireHarness(t)
			args := []string{"chat", "--component=engine", "--revision=aaaaaaaa"}
			if scenario != "nonTTY" {
				args = append(args, "--yes")
			}
			switch scenario {
			case "pending empty":
				h.ir.Annotations[constants.ReleaseHeldRevisionAnnotationKey] = ""
			case "pending false":
				h.ir.Annotations[constants.ReleaseHeldRevisionAnnotationKey] = "false"
			case "pending same":
				h.ir.Annotations[constants.ReleaseHeldRevisionAnnotationKey] = "chat-engine-aaaaaaaa"
			case "missing provenance":
				delete(h.ir.Annotations, constants.InferenceReplicaControllerWriteAnnotationKey)
			case "unknown block":
				h.ir.Status.RetryBlocks = append(h.ir.Status.RetryBlocks, v1beta1.RetryBlock{TargetRevision: "chat-engine-bbbbbbbb", State: "PRIVATE_STATE", AttemptsStarted: 1})
			}
			out, stderr, err := h.execute(t, args...)
			require.Error(t, err)
			require.Empty(t, out)
			require.Zero(t, h.patches)
			require.NotContains(t, stderr+err.Error(), "PRIVATE")
			require.Equal(t, 1, h.parentReads)
		})
	}
}

func TestReleaseHeldErrorsAndResponsesNeverReplayOrLeak(t *testing.T) {
	for _, scenario := range []string{"409", "422 guard", "422 admission", "429", "503", "307", "wrong UID", "wrong GVK", "missing RV", "malformed", "oversized", "kind alias", "uid alias", "duplicate kind", "duplicate UID", "metadata alias", "metadata null", "trailing document"} {
		t.Run(scenario, func(t *testing.T) {
			h := newHeldWireHarness(t)
			h.patchReply = func(w http.ResponseWriter, r *http.Request) {
				metadata := `{"name":"actual-native-engine","namespace":"prod","uid":"uid-ir","resourceVersion":"82"}`
				ambiguous := ""
				switch scenario {
				case "kind alias":
					ambiguous = `{"apiVersion":"ome.io/v1beta1","kind":"Secret","Kind":"InferenceReplica","metadata":` + metadata + `}`
				case "uid alias":
					ambiguous = `{"apiVersion":"ome.io/v1beta1","kind":"InferenceReplica","metadata":{"name":"actual-native-engine","namespace":"prod","uid":"wrong-uid","UID":"uid-ir","resourceVersion":"82"}}`
				case "duplicate kind":
					ambiguous = `{"apiVersion":"ome.io/v1beta1","kind":"Secret","kind":"InferenceReplica","metadata":` + metadata + `}`
				case "duplicate UID":
					ambiguous = `{"apiVersion":"ome.io/v1beta1","kind":"InferenceReplica","metadata":{"name":"actual-native-engine","namespace":"prod","uid":"wrong-uid","uid":"uid-ir","resourceVersion":"82"}}`
				case "metadata alias":
					ambiguous = `{"apiVersion":"ome.io/v1beta1","kind":"InferenceReplica","metadata":null,"Metadata":` + metadata + `}`
				case "metadata null":
					ambiguous = `{"apiVersion":"ome.io/v1beta1","kind":"InferenceReplica","metadata":null}`
				case "trailing document":
					ambiguous = `{"apiVersion":"ome.io/v1beta1","kind":"InferenceReplica","metadata":` + metadata + `}{}`
				}
				if ambiguous != "" {
					_, _ = io.WriteString(w, ambiguous)
					return
				}
				if scenario == "307" && h.patches == 1 {
					w.Header().Set("Location", r.URL.Path+"?redirected=true")
					w.WriteHeader(http.StatusTemporaryRedirect)
					return
				}
				code := 0
				switch scenario {
				case "409":
					code = 409
				case "422 guard", "422 admission":
					code = 422
				case "429":
					code = 429
				case "503":
					code = 503
				}
				if code != 0 {
					w.Header().Set("Retry-After", "0")
					w.WriteHeader(code)
					reason := metav1.StatusReasonInvalid
					if code == 409 {
						reason = metav1.StatusReasonConflict
					}
					status := metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Code: int32(code), Reason: reason, Message: "PRIVATE_API_MESSAGE"}
					if scenario == "422 guard" {
						status.Message = "the server rejected our request due to an error in our request"
						status.Details = &metav1.StatusDetails{}
					}
					if scenario == "422 admission" {
						status.Details = &metav1.StatusDetails{Causes: []metav1.StatusCause{{Field: "metadata.annotations", Message: "PRIVATE_CAUSE"}}}
					}
					require.NoError(t, json.NewEncoder(w).Encode(status))
					return
				}
				response := h.ir.DeepCopy()
				switch scenario {
				case "wrong UID":
					response.UID = "wrong-uid"
				case "wrong GVK":
					response.Kind = "InferenceService"
				case "missing RV":
					response.ResourceVersion = ""
				case "malformed":
					_, _ = io.WriteString(w, "PRIVATE_NOT_JSON")
					return
				case "oversized":
					_, _ = io.WriteString(w, strings.Repeat("PRIVATE", 180000))
					return
				}
				require.NoError(t, json.NewEncoder(w).Encode(response))
			}
			out, stderr, err := h.execute(t, "chat", "--component=engine", "--revision=aaaaaaaa", "--yes", "-o", "json")
			require.Error(t, err)
			require.Empty(t, out)
			require.Equal(t, 1, h.patches)
			require.NotContains(t, stderr+err.Error(), "PRIVATE")
			if scenario == "409" || scenario == "422 guard" {
				require.Equal(t, 3, exitcode.FromError(err))
			} else {
				require.Equal(t, 1, exitcode.FromError(err))
			}
		})
	}
}

type heldPrivateWriter struct{}

func (heldPrivateWriter) Write([]byte) (int, error) { return 0, errors.New("PRIVATE_IO_MESSAGE") }

func TestReleaseHeldIOFailuresAndContextRefuse(t *testing.T) {
	for _, scenario := range []string{"preview", "result", "canceled", "shorter timeout"} {
		t.Run(scenario, func(t *testing.T) {
			h := newHeldWireHarness(t)
			var out, stderr bytes.Buffer
			streams := genericiooptions.IOStreams{In: bytes.NewBuffer(nil), Out: &out, ErrOut: &stderr}
			if scenario == "preview" {
				streams.ErrOut = heldPrivateWriter{}
			}
			if scenario == "result" {
				streams.Out = heldPrivateWriter{}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "canceled" {
				cancel()
			}
			if scenario == "shorter timeout" {
				h.f.config.Timeout = time.Nanosecond
			}
			cmd := newReleaseHeldCmd(h.f, streams, reportv1alpha1.SystemClock{})
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs([]string{"chat", "--component=engine", "--revision=aaaaaaaa", "--yes"})
			err := cmd.ExecuteContext(ctx)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "PRIVATE")
			require.Empty(t, out.String())
			if scenario == "result" {
				require.Equal(t, 1, h.patches)
			} else {
				require.Zero(t, h.patches)
			}
		})
	}
}

type heldDeadlineClient struct {
	ctrlclient.Client
	t   *testing.T
	cap time.Duration
}

func (c heldDeadlineClient) Get(ctx context.Context, key ctrlclient.ObjectKey, value ctrlclient.Object, opts ...ctrlclient.GetOption) error {
	deadline, ok := ctx.Deadline()
	require.True(c.t, ok)
	require.LessOrEqual(c.t, time.Until(deadline), c.cap)
	return c.Client.Get(ctx, key, value, opts...)
}

func TestReleaseHeldPreservesShorterRuntimeRequestDeadline(t *testing.T) {
	h := newHeldWireHarness(t)
	h.f.config.Timeout = 500 * time.Millisecond
	h.f.Runtime = heldDeadlineClient{Client: h.f.Runtime, t: t, cap: 500 * time.Millisecond}
	_, _, err := h.execute(t, "chat", "--component=engine", "--revision=aaaaaaaa", "--yes", "--dry-run=client")
	require.NoError(t, err)
}
