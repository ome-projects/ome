package scale

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/yaml"
)

const privateSentinel = "private-scale-credential-sentinel"

type nativeAPI struct {
	mu                sync.Mutex
	parent            *v1beta1.InferenceService
	runtime           *v1beta1.ServingRuntime
	replica           *v1beta1.InferenceReplica
	scale             *autoscalingv1.Scale
	paths             []string
	patches           int
	patchStatus       int
	patchResponse     string
	patchDelay        time.Duration
	changeFinalParent bool
	parentReads       int
}

func nativeFixture() *nativeAPI {
	mode := constants.OMENative
	kind := "ServingRuntime"
	p := &v1beta1.InferenceService{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"}, ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: "uid-chat", ResourceVersion: "42", Generation: 7},
		Spec: v1beta1.InferenceServiceSpec{DeploymentMode: &mode, Runtime: &v1beta1.ServingRuntimeRef{Name: "simple", Kind: &kind, AutoSync: ptr.To(true)}, Engine: &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1), MaxReplicas: 10, Autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}}}}}
	p.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{v1beta1.EngineComponent: {RolloutPhase: v1beta1.RolloutPhaseStable, ScaleTargetRef: &v1beta1.ScaleTargetRef{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "chat-engine"}, Autoscaler: &v1beta1.ComponentAutoscalerStatus{Class: v1beta1.AutoscalerNone, ManagedBy: "none", SpecSource: "isvc"}, Lifecycle: &v1beta1.LifecycleStatus{CurrentRevision: "chat-engine-aaaaaaaa", UpdateRevision: "chat-engine-aaaaaaaa"}}}
	rt := &v1beta1.ServingRuntime{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "ServingRuntime"}, ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: "prod", UID: "uid-runtime", ResourceVersion: "99", Generation: 1}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "busybox:1.36", Env: []corev1.EnvVar{{Name: "SYNTHETIC_SECRET", Value: privateSentinel}}}}}}}
	r := &v1beta1.InferenceReplica{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica"}, ObjectMeta: metav1.ObjectMeta{Name: "chat-engine", Namespace: "prod", UID: "uid-ir", ResourceVersion: "81", Generation: 2, Annotations: map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: "7"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "chat", UID: p.UID, Controller: ptr.To(true)}}}, Spec: v1beta1.InferenceReplicaSpec{ParentRef: v1beta1.ParentReference{Name: "chat"}, Component: v1beta1.EngineComponent, Replicas: ptr.To[int32](1), Autoscaler: p.Spec.Engine.Autoscaler.DeepCopy()}, Status: v1beta1.InferenceReplicaStatus{ObservedGeneration: 2, Replicas: 1, ReadyReplicas: 1, ServingReplicas: 1, AvailableReplicas: 1, CurrentRevision: "chat-engine-aaaaaaaa", UpdateRevision: "chat-engine-aaaaaaaa", InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}}}}
	s := &autoscalingv1.Scale{TypeMeta: metav1.TypeMeta{APIVersion: "autoscaling/v1", Kind: "Scale"}, ObjectMeta: metav1.ObjectMeta{Name: r.Name, Namespace: r.Namespace, UID: r.UID, ResourceVersion: r.ResourceVersion}, Spec: autoscalingv1.ScaleSpec{Replicas: 1}, Status: autoscalingv1.ScaleStatus{Replicas: 1, Selector: privateSentinel}}
	return &nativeAPI{parent: p, runtime: rt, replica: r, scale: s}
}

func (a *nativeAPI) handler(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.paths = append(a.paths, r.Method+" "+r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Add("Warning", `299 synthetic "`+privateSentinel+`"`)
	write := func(value any) { _ = json.NewEncoder(w).Encode(value) }
	switch r.URL.Path {
	case "/api":
		write(metav1.APIVersions{Versions: []string{"v1"}})
	case "/apis":
		write(metav1.APIGroupList{Groups: []metav1.APIGroup{{Name: "ome.io", Versions: []metav1.GroupVersionForDiscovery{{GroupVersion: "ome.io/v1beta1", Version: "v1beta1"}}, PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "ome.io/v1beta1", Version: "v1beta1"}}}})
	case "/api/v1":
		write(metav1.APIResourceList{GroupVersion: "v1"})
	case "/apis/ome.io/v1beta1":
		write(metav1.APIResourceList{GroupVersion: "ome.io/v1beta1", APIResources: []metav1.APIResource{{Name: "inferenceservices", Namespaced: true, Kind: "InferenceService", Verbs: []string{"get"}}, {Name: "servingruntimes", Namespaced: true, Kind: "ServingRuntime", Verbs: []string{"get", "list"}}, {Name: "clusterservingruntimes", Kind: "ClusterServingRuntime", Verbs: []string{"get", "list"}}, {Name: "inferencereplicas", Namespaced: true, Kind: "InferenceReplica", Verbs: []string{"get"}}}})
	case "/apis/ome.io/v1beta1/namespaces/prod/inferenceservices/chat":
		a.parentReads++
		p := a.parent.DeepCopy()
		if a.changeFinalParent && a.parentReads > 1 {
			p.ResourceVersion = "changed"
		}
		write(p)
	case "/apis/ome.io/v1beta1/namespaces/prod/servingruntimes/simple":
		write(a.runtime)
	case "/apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine":
		write(a.replica)
	case "/apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine/scale":
		if r.Method == "GET" {
			write(a.scale)
			return
		}
		a.patches++
		requirePatch := r.Method == "PATCH" && r.Header.Get("Content-Type") == "application/json-patch+json"
		body, _ := io.ReadAll(r.Body)
		expected := `[{"op":"test","path":"/metadata/uid","value":"uid-ir"},{"op":"test","path":"/metadata/resourceVersion","value":"81"},{"op":"replace","path":"/spec/replicas","value":3}]`
		if !requirePatch || string(body) != expected {
			w.WriteHeader(400)
			write(metav1.Status{Status: "Failure", Reason: metav1.StatusReasonBadRequest, Message: privateSentinel, Code: 400})
			return
		}
		if a.patchDelay > 0 {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(a.patchDelay):
			}
		}
		if a.patchStatus != 0 {
			w.Header().Set("Retry-After", "0")
			if a.patchStatus >= 300 && a.patchStatus < 400 {
				w.Header().Set("Location", "/api/v1/namespaces/prod/secrets/unselected")
			}
			w.WriteHeader(a.patchStatus)
			_, _ = io.WriteString(w, a.patchResponse)
			return
		}
		if a.patchResponse != "" {
			_, _ = io.WriteString(w, a.patchResponse)
			return
		}
		response := a.scale.DeepCopy()
		response.Spec.Replicas = 3
		response.ResourceVersion = "82"
		write(response)
		if r.URL.Query().Get("dryRun") != "All" {
			a.scale = response
		}
	default:
		w.WriteHeader(404)
		write(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Reason: metav1.StatusReasonNotFound, Message: privateSentinel, Code: 404})
	}
}

func (a *nativeAPI) observed() (int, int, []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.patches, a.parentReads, append([]string(nil), a.paths...)
}

func nativeConfig(t *testing.T, url string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "synthetic-kubeconfig")
	body := fmt.Sprintf("apiVersion: v1\nkind: Config\nclusters:\n- name: synthetic\n  cluster:\n    server: %s\ncontexts:\n- name: synthetic\n  context:\n    cluster: synthetic\n    namespace: prod\ncurrent-context: synthetic\nusers: []\n", url)
	require.NoError(t, os.WriteFile(path, []byte(body), 0600))
	return path
}

type nativeFactory struct {
	factory.Factory
	cachedCalls int
}

func (f *nativeFactory) ContextName() (string, error) {
	return f.Factory.(factory.ContextResolver).ContextName()
}
func (f *nativeFactory) OMEClient() (versioned.Interface, error) {
	f.cachedCalls++
	return nil, errors.New(privateSentinel)
}
func (f *nativeFactory) KubeClient() (kubernetes.Interface, error) {
	f.cachedCalls++
	return nil, errors.New(privateSentinel)
}
func (f *nativeFactory) OMEClientForAction(ctx context.Context) (versioned.Interface, error) {
	return f.Factory.(factory.ActionReadClientsResolver).OMEClientForAction(ctx)
}
func (f *nativeFactory) KubeClientForAction(ctx context.Context) (kubernetes.Interface, error) {
	return f.Factory.(factory.ActionReadClientsResolver).KubeClientForAction(ctx)
}
func (f *nativeFactory) RuntimeClientForAction(ctx context.Context) (ctrlclient.Client, error) {
	return f.Factory.(factory.ActionRuntimeResolver).RuntimeClientForAction(ctx)
}

func runNativeCommand(t *testing.T, api *nativeAPI, args []string) (string, string, error, *nativeFactory) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(api.handler))
	t.Cleanup(server.Close)
	flags := genericclioptions.NewConfigFlags(true)
	config := nativeConfig(t, server.URL)
	flags.KubeConfig = &config
	timeout := "2s"
	flags.Timeout = &timeout
	f := &nativeFactory{Factory: factory.New(flags)}
	var out, stderr bytes.Buffer
	cmd := newCmd(f, genericiooptions.IOStreams{In: bytes.NewBuffer(nil), Out: &out, ErrOut: &stderr}, reportv1alpha1.ClockFunc(func() time.Time { return time.Date(2026, 9, 15, 21, 0, 0, 0, time.UTC) }))
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), stderr.String(), err, f
}

func TestScaleRealLocalhostFourFormatsAndThreeDryModes(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		for _, dry := range []string{"none", "client", "server"} {
			t.Run(format+"/"+dry, func(t *testing.T) {
				api := nativeFixture()
				out, stderr, err, f := runNativeCommand(t, api, []string{"chat", "--component=engine", "--replicas=3", "--override-autoscaler", "--yes", "--dry-run=" + dry, "-o", format})
				require.NoError(t, err)
				require.Zero(t, f.cachedCalls)
				require.Equal(t, map[bool]int{true: 0, false: 1}[dry == "client"], api.patches)
				require.GreaterOrEqual(t, api.parentReads, 2)
				require.Contains(t, stderr, "ALPHA guarded scale preview")
				require.NotContains(t, out+stderr, privateSentinel)
				require.NotContains(t, out+stderr, "synthetic-kubeconfig")
				if format == "json" || format == "yaml" {
					var result reportv1alpha1.ActionResult
					if format == "json" {
						require.NoError(t, json.Unmarshal([]byte(out), &result))
					} else {
						require.NoError(t, yaml.Unmarshal([]byte(out), &result))
					}
					require.Equal(t, reportv1alpha1.APIVersion, result.APIVersion)
					require.Equal(t, "scale", result.Action)
					require.Equal(t, dry != "client", result.Accepted)
					require.Equal(t, dry == "none", result.Applied)
					require.Equal(t, int32(3), result.Scale.RequestedReplicas)
					require.Equal(t, "Unverifiable", result.Scale.ParentFreshness)
					require.Equal(t, "None", result.Scale.Class)
					require.Contains(t, result.FollowUp, "kubectl ome autoscale status chat -n prod --context=synthetic")
				}
				if format == "table" || format == "wide" {
					for _, line := range strings.Split(out, "\n") {
						require.LessOrEqual(t, len(line), 80, line)
					}
				}
			})
		}
	}
}

func TestScaleRealLocalhostFinalSourceDriftAndDefaultRefusal(t *testing.T) {
	for _, mode := range []string{"drift", "default", "zero"} {
		t.Run(mode, func(t *testing.T) {
			api := nativeFixture()
			args := []string{"chat", "--component=engine", "--replicas=3", "--yes", "-o", "json"}
			if mode == "drift" {
				api.changeFinalParent = true
				args = append(args, "--override-autoscaler")
			} else if mode == "zero" {
				args = []string{"chat", "--component=engine", "--replicas=0", "--yes"}
			}
			out, stderr, err, _ := runNativeCommand(t, api, args)
			require.Error(t, err)
			require.Empty(t, out)
			require.Zero(t, api.patches)
			require.NotContains(t, stderr+err.Error(), privateSentinel)
		})
	}
}

// Root supplies the built actual CLI after registration. This test never
// reaches a cluster and exercises the native process against the same API.
func TestScaleActualBinaryLocalhostMatrix(t *testing.T) {
	binary := os.Getenv("OME_SCALE_BINARY")
	if binary == "" {
		t.Skip("root must supply the actual built CLI through OME_SCALE_BINARY")
	}
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		for _, dry := range []string{"none", "client", "server"} {
			t.Run(format+"/"+dry, func(t *testing.T) {
				api := nativeFixture()
				server := httptest.NewServer(http.HandlerFunc(api.handler))
				defer server.Close()
				config := nativeConfig(t, server.URL)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, binary, "--kubeconfig", config, "scale", "chat", "--component=engine", "--replicas=3", "--override-autoscaler", "--yes", "--dry-run="+dry, "-o", format)
				var out, stderr bytes.Buffer
				cmd.Stdout = &out
				cmd.Stderr = &stderr
				require.NoError(t, cmd.Run(), stderr.String())
				require.NotContains(t, out.String()+stderr.String(), privateSentinel)
				require.Equal(t, map[bool]int{true: 0, false: 1}[dry == "client"], api.patches)
			})
		}
	}
}

func TestScaleRealLocalhostMalformedAndAPIRejectionsNeverReplay(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		code   int
	}{
		{"malformed", 0, `{"metadata":`, 1},
		{"duplicate identity", 0, `{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{"name":"chat-engine","name":"chat-engine","namespace":"prod","uid":"uid-ir","resourceVersion":"82"},"spec":{"replicas":3}}`, 1},
		{"mixed alias", 0, `{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{"name":"chat-engine","namespace":"prod","uid":"uid-ir","UID":"uid-ir","resourceVersion":"82"},"spec":{"replicas":3}}`, 1},
		{"wrong UID", 0, `{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{"name":"chat-engine","namespace":"prod","uid":"other","resourceVersion":"82"},"spec":{"replicas":3}}`, 1},
		{"wrong replicas", 0, `{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{"name":"chat-engine","namespace":"prod","uid":"uid-ir","resourceVersion":"82"},"spec":{"replicas":4}}`, 1},
		{"oversize success", 0, `{"private":"` + strings.Repeat("x", (1<<20)+1) + `"}`, 1},
		{"oversize error", 500, strings.Repeat("x", (1<<20)+1), 1},
		{"conflict", 409, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Conflict","message":"` + privateSentinel + `","code":409}`, 3},
		{"generic JSON test 422", 422, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Invalid","message":"the server rejected our request due to an error in our request","details":{},"code":422}`, 3},
		{"ordinary admission 422", 422, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Invalid","message":"` + privateSentinel + `","details":{"name":"chat-engine","causes":[{"reason":"FieldValueInvalid","field":"spec.replicas"}]},"code":422}`, 1},
		{"forbidden", 403, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","message":"` + privateSentinel + `","code":403}`, 1},
		{"retry after", 429, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"TooManyRequests","message":"` + privateSentinel + `","code":429}`, 1},
	}
	for _, status := range []int{301, 302, 303, 307, 308} {
		cases = append(cases, struct {
			name   string
			status int
			body   string
			code   int
		}{fmt.Sprint("redirect", status), status, `{}`, 1})
	}
	for _, dry := range []string{"none", "server"} {
		for _, tc := range cases {
			t.Run(dry+"/"+tc.name, func(t *testing.T) {
				api := nativeFixture()
				api.patchStatus = tc.status
				api.patchResponse = tc.body
				out, stderr, err, _ := runNativeCommand(t, api, []string{"chat", "--component=engine", "--replicas=3", "--override-autoscaler", "--yes", "--dry-run=" + dry, "-o", "json"})
				require.Error(t, err)
				require.Equal(t, tc.code, exitcode.FromError(err))
				require.Empty(t, out)
				require.NotContains(t, stderr+err.Error(), privateSentinel)
				patches, _, paths := api.observed()
				require.Equal(t, 1, patches)
				for _, path := range paths {
					require.NotContains(t, path, "secrets")
					require.NotContains(t, path, "PUT")
				}
			})
		}
	}
}
