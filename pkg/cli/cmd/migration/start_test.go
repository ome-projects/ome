package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
	"sigs.k8s.io/ome/pkg/constants"
)

// Catches missing action registration and acquiring clients before validation.
func TestStartRequiresExplicitComponentBeforeAcquisition(t *testing.T) {
	var out, stderr bytes.Buffer
	cmd := NewCmd(factory.Static{}, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetArgs([]string{"start", "chat", "--instance=0", "--yes"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "component is required") {
		t.Fatalf("required local component validation missing: %v", err)
	}
	if out.Len() != 0 || stderr.Len() != 0 {
		t.Fatal("local validation produced action output")
	}
}

type startFactory struct {
	factory.Static
	config  *rest.Config
	failure string
}

func (f startFactory) RESTConfig() (*rest.Config, error) {
	if f.failure == "config" {
		return nil, errors.New("private secret")
	}
	if f.failure == "nil config" {
		return nil, nil
	}
	return rest.CopyConfig(f.config), nil
}
func (f startFactory) Namespace() (string, bool, error) {
	if f.failure == "namespace" {
		return "", false, errors.New("private secret")
	}
	return f.Static.Namespace()
}
func (f startFactory) ContextName() (string, error) {
	if f.failure == "context" {
		return "", errors.New("private secret")
	}
	return f.Static.ContextName()
}

type startFixture struct {
	parent                                         *v1beta1.InferenceService
	ir                                             *v1beta1.InferenceReplica
	pods                                           []corev1.Pod
	cr                                             *appsv1.ControllerRevision
	audit                                          *corev1.ConfigMap
	patchStatus                                    *metav1.Status
	changedIR, badResponse, previewError, canceled bool
	factoryFailure, responseKind                   string
	outputError                                    bool
}

type failingStartWriter struct{}

func (failingStartWriter) Write([]byte) (int, error) { return 0, errors.New("private IO failure") }

func newStartFixture() startFixture {
	controller, autosync := true, true
	kind := "ServingRuntime"
	mode := constants.OMENative
	replicas := int32(1)
	parent := &v1beta1.InferenceService{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"}, ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: "uid-chat", ResourceVersion: "42", Generation: 7, Annotations: map[string]string{"preserve": "private"}}, Spec: v1beta1.InferenceServiceSpec{DeploymentMode: &mode, Runtime: &v1beta1.ServingRuntimeRef{Name: "simple", Kind: &kind, AutoSync: &autosync}, Engine: &v1beta1.EngineSpec{}}}
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: "chat-engine", Namespace: "prod", UID: "uid-ir", ResourceVersion: "81", Generation: 2, Labels: map[string]string{constants.InferenceServiceLabel: "chat"}, Annotations: map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: "7"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "chat", UID: "uid-chat", Controller: &controller}}}, Spec: v1beta1.InferenceReplicaSpec{Component: v1beta1.EngineComponent, ParentRef: v1beta1.ParentReference{Name: "chat"}, Replicas: &replicas, Runners: []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}}, Status: v1beta1.InferenceReplicaStatus{ObservedGeneration: 2, Replicas: 1, CurrentRevision: "chat-engine-aaaaaaaa", UpdateRevision: "chat-engine-aaaaaaaa", InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 3, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "chat-engine-aaaaaaaa", NodesOccupied: []string{"forged-node"}}}}}
	owner := []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "chat-engine", UID: "uid-ir", Controller: &controller}}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "chat-engine-3-default-0", Namespace: "prod", UID: "uid-pod", ResourceVersion: "71", OwnerReferences: owner, Labels: map[string]string{constants.InferenceServicePodLabelKey: "chat", constants.OMEComponentLabel: "engine", "ome.io/managed-by": "OMENative", "ome.io/instance-index": "3", "ome.io/instance-incarnation": "1", "ome.io/pod-ordinal": "0", "ome.io/runner": "default", "ome.io/revision-hash": "aaaaaaaa"}}, Spec: corev1.PodSpec{NodeName: "node-a"}}
	cr := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "chat-engine-aaaaaaaa", Namespace: "prod", UID: "uid-cr", ResourceVersion: "61", OwnerReferences: owner, Labels: map[string]string{constants.InferenceServicePodLabelKey: "chat", constants.OMEComponentLabel: "engine", "ome.io/managed-by": "OMENative"}}, Data: runtime.RawExtension{Raw: []byte(`{"podSpec":{"containers":[{"name":"main","image":"busybox"}]}}`)}}
	return startFixture{parent: parent, ir: ir, pods: []corev1.Pod{pod}, cr: cr}
}

func runStartWire(t *testing.T, fixture startFixture, suffix ...string) (error, reportv1alpha1.ActionResult, int, string) {
	t.Helper()
	patches := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/inferenceservices/chat"):
			require.NoError(t, json.NewEncoder(w).Encode(fixture.parent))
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/inferencereplicas"):
			require.NoError(t, json.NewEncoder(w).Encode(v1beta1.InferenceReplicaList{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplicaList"}, Items: []v1beta1.InferenceReplica{*fixture.ir}}))
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/inferencereplicas/chat-engine"):
			ir := fixture.ir.DeepCopy()
			if fixture.changedIR {
				ir.ResourceVersion = "82"
			}
			require.NoError(t, json.NewEncoder(w).Encode(ir))
		case r.Method == "PATCH":
			patches++
			if fixture.patchStatus != nil {
				status := fixture.patchStatus.DeepCopy()
				status.TypeMeta = metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}
				w.WriteHeader(int(fixture.patchStatus.Code))
				require.NoError(t, json.NewEncoder(w).Encode(status))
				return
			}
			require.Equal(t, "/apis/ome.io/v1beta1/namespaces/prod/inferenceservices/chat", r.URL.Path)
			require.Equal(t, "application/json-patch+json", r.Header.Get("Content-Type"))
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			var ops []struct {
				Op, Path string
				Value    any
			}
			require.NoError(t, json.Unmarshal(body, &ops))
			require.Equal(t, "test", ops[0].Op)
			require.Equal(t, "/metadata/uid", ops[0].Path)
			require.Equal(t, "uid-chat", ops[0].Value)
			require.Equal(t, "/metadata/resourceVersion", ops[1].Path)
			require.Equal(t, "42", ops[1].Value)
			requestOp := 2
			if fixture.parent.Annotations == nil {
				require.Len(t, ops, 4)
				require.Equal(t, "/metadata/annotations", ops[2].Path)
				requestOp = 3
			} else {
				require.Len(t, ops, 3)
			}
			require.True(t, strings.HasPrefix(ops[requestOp].Path, "/metadata/annotations/ome.io~1migration-request-v1-"))
			var payload map[string]any
			require.NoError(t, json.Unmarshal([]byte(ops[requestOp].Value.(string)), &payload))
			require.Equal(t, "v1", payload["schemaVersion"])
			require.Equal(t, float64(3), payload["instance"])
			require.Equal(t, "engine", payload["component"])
			require.Equal(t, "node-a", payload["from_node"])
			require.Equal(t, "kubectl-ome", payload["requested_by"])
			patch, err := jsonpatch.DecodePatch(body)
			require.NoError(t, err)
			original, err := json.Marshal(fixture.parent)
			require.NoError(t, err)
			updated, err := patch.Apply(original)
			require.NoError(t, err)
			var response v1beta1.InferenceService
			require.NoError(t, json.Unmarshal(updated, &response))
			require.Equal(t, fixture.parent.Spec, response.Spec)
			require.Equal(t, fixture.parent.Status, response.Status)
			if fixture.parent.Annotations != nil {
				require.Equal(t, "private", response.Annotations["preserve"])
			}
			response.ResourceVersion = "43"
			if fixture.badResponse {
				response.UID = "replaced"
			}
			if fixture.responseKind == "corrupt" {
				_, _ = io.WriteString(w, `{`)
				return
			}
			if fixture.responseKind == "aliased kind" {
				response.Kind = "Secret"
				body, err := json.Marshal(response)
				require.NoError(t, err)
				_, _ = io.WriteString(w, strings.TrimSuffix(string(body), "}")+`,"Kind":"InferenceService"}`)
				return
			}
			if fixture.responseKind == "oversized" {
				_, _ = io.WriteString(w, strings.Repeat("x", 1048577))
				return
			}
			require.NoError(t, json.NewEncoder(w).Encode(response))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer server.Close()
	config := &rest.Config{Host: server.URL}
	ome, err := versioned.NewForConfig(config)
	require.NoError(t, err)
	objects := []runtime.Object{fixture.cr}
	for i := range fixture.pods {
		objects = append(objects, &fixture.pods[i])
	}
	if fixture.audit != nil {
		objects = append(objects, fixture.audit)
	}
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	rt := &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: "prod", UID: "uid-runtime", ResourceVersion: "99", Generation: 1}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "busybox"}}}}}
	f := startFactory{Static: factory.Static{OME: ome, Kube: kubefake.NewClientset(objects...), Runtime: clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(rt).Build(), NS: "prod", Context: "moirai"}, config: config, failure: fixture.factoryFailure}
	if fixture.factoryFailure == "OME" {
		f.OME = nil
	}
	if fixture.factoryFailure == "Kube" {
		f.Kube = nil
	}
	if fixture.factoryFailure == "runtime" {
		f.Runtime = nil
	}
	if fixture.factoryFailure == "transport" {
		f.config = &rest.Config{Host: "%invalid"}
	}
	var out, stderr bytes.Buffer
	var errOut io.Writer = &stderr
	if fixture.previewError {
		errOut = failingStartWriter{}
	}
	var stdout io.Writer = &out
	if fixture.outputError {
		stdout = failingStartWriter{}
	}
	cmd := NewCmd(f, genericiooptions.IOStreams{Out: stdout, ErrOut: errOut})
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	ctx := context.Background()
	if fixture.canceled {
		c, cancel := context.WithCancel(ctx)
		cancel()
		ctx = c
	}
	cmd.SetContext(ctx)
	cmd.SetArgs(append([]string{"start", "chat", "--component=engine", "--instance=3", "--yes"}, suffix...))
	err = cmd.Execute()
	var result reportv1alpha1.ActionResult
	if out.Len() > 0 {
		require.NoError(t, json.Unmarshal(out.Bytes(), &result))
	}
	return err, result, patches, stderr.String()
}

func TestStartLookupExactMailboxNoReplayOrPrompt(t *testing.T) {
	id := "12345678-1234-4123-8123-123456789abc"
	for _, tc := range []struct {
		name, raw, extra string
		code             int
	}{{"retained exact", `{"schemaVersion":"v1","component":"engine","instance":3,"from_node":"node-a","requested_at":"2026-09-15T21:00:00Z","requested_by":"kubectl-ome"}`, "", 0}, {"payload changed", `{"schemaVersion":"v1","component":"engine","instance":3,"from_node":"node-a","reason":"changed","requested_by":"kubectl-ome"}`, "", 3}, {"unseen", "", "", 3}, {"unknown payload", `{"schemaVersion":"v1","component":"engine","instance":3,"from_node":"node-a","unknown":true}`, "", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStartFixture()
			if tc.raw != "" {
				f.parent.Annotations[constants.MigrationRequestAnnotationPrefix+id] = tc.raw
			}
			f.parent.Annotations[constants.PausedRolloutAnnotation] = "freeze"
			err, result, patches, _ := runStartWire(t, f, "-o=json", "--request-id="+id, "--yes=false")
			require.Equal(t, tc.code, exitcode.FromError(err))
			require.Zero(t, patches)
			if tc.code == 0 {
				require.Equal(t, id, result.RequestID)
				require.False(t, result.Accepted)
				require.False(t, result.Applied)
				require.Contains(t, result.Message, "no replay")
			} else {
				require.Empty(t, result.Kind)
			}
		})
	}
}

func TestStartGuardedRejectionAndAmbiguousResponseNoReplay(t *testing.T) {
	canonical := apierrors.NewGenericServerResponse(422, "", schema.GroupResource{}, "", "private API text", 0, false).ErrStatus
	for _, tc := range []struct {
		name   string
		status *metav1.Status
		code   int
	}{{"conflict", &metav1.Status{Status: "Failure", Reason: metav1.StatusReasonConflict, Code: 409, Message: "private API text"}, 3}, {"generic application 422", &canonical, 3}, {"admission 422", &metav1.Status{Status: "Failure", Reason: metav1.StatusReasonInvalid, Code: 422, Message: "testing value /metadata/uid failed private API text", Details: &metav1.StatusDetails{Name: "chat"}}, 1}, {"forbidden", &metav1.Status{Status: "Failure", Reason: metav1.StatusReasonForbidden, Code: 403, Message: "private API text"}, 1}, {"unbound response", nil, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStartFixture()
			f.patchStatus = tc.status
			f.badResponse = tc.status == nil
			err, result, patches, preview := runStartWire(t, f, "-o=json")
			require.Equal(t, tc.code, exitcode.FromError(err))
			require.Equal(t, 1, patches)
			require.Empty(t, result.Kind)
			require.NotContains(t, err.Error(), "private API text")
			if tc.code == 1 {
				require.Contains(t, err.Error(), "inspect migration status using the preview UUID")
			}
			require.NotContains(t, preview, "private API text")
		})
	}
}

func TestStartRechecksAndIOBeforeAnyResultOrPatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		edit   func(*startFixture)
		suffix string
		code   int
	}{{"client fresh IR", func(f *startFixture) { f.changedIR = true }, "--dry-run=client", 3}, {"normal fresh IR", func(f *startFixture) { f.changedIR = true }, "--dry-run=none", 3}, {"preview writer", func(f *startFixture) { f.previewError = true }, "--dry-run=none", 1}, {"cancel", func(f *startFixture) { f.canceled = true }, "--dry-run=none", 1}, {"nonTTY confirmation", func(*startFixture) {}, "--yes=false", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStartFixture()
			tc.edit(&f)
			err, result, patches, _ := runStartWire(t, f, "-o=json", tc.suffix)
			require.Equal(t, tc.code, exitcode.FromError(err))
			require.Zero(t, patches)
			require.Empty(t, result.Kind)
		})
	}
}

func TestStartNilAnnotationsWire(t *testing.T) {
	f := newStartFixture()
	f.parent.Annotations = nil
	err, result, patches, _ := runStartWire(t, f, "-o=json")
	require.NoError(t, err)
	require.True(t, result.Applied)
	require.Equal(t, 1, patches)
}

type noAcquireStartFactory struct{ factory.Static }

func (noAcquireStartFactory) Namespace() (string, bool, error) {
	panic("local parser acquired namespace")
}

func TestStartPrivateLocalParserZeroAcquisition(t *testing.T) {
	for _, suffix := range [][]string{{"--instance=00"}, {"--instance=2147483648"}, {"--component=private secret"}, {"--from-node=private secret"}, {"--request-id=private-secret"}, {"--reason=Bearer private-secret"}, {"--requested-by=user:private-secret@host"}, {"--hint-node=node-a,node-a"}, {"--dry-run=private-secret"}, {"-o=private-secret"}, {"--unknown=private-secret"}, {"--yes=private-secret"}} {
		var out, stderr bytes.Buffer
		cmd := NewCmd(noAcquireStartFactory{}, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
		cmd.SilenceUsage, cmd.SilenceErrors = true, true
		cmd.SetArgs(append([]string{"start", "chat", "--component=engine", "--instance=3"}, suffix...))
		err := cmd.Execute()
		require.Error(t, err)
		require.Equal(t, 1, exitcode.FromError(err))
		require.NotContains(t, err.Error(), "private-secret")
		require.Zero(t, out.Len())
		require.Zero(t, stderr.Len())
	}
}

func TestStartInvalidOMENamespaceBeforeAcquisition(t *testing.T) {
	var out, stderr bytes.Buffer
	cmd := NewCmd(noAcquireStartFactory{}, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	cmd.SetArgs([]string{"start", "chat", "--component=engine", "--instance=3", "--ome-namespace=private-secret!"})
	require.NotPanics(t, func() {
		err := cmd.Execute()
		require.Error(t, err)
		require.NotContains(t, err.Error(), "private-secret")
	})
	require.Zero(t, out.Len())
	require.Zero(t, stderr.Len())
}

func TestStartAcquisitionAndResponseFailures(t *testing.T) {
	for _, failure := range []string{"namespace", "context", "OME", "Kube", "runtime", "config", "nil config", "transport", "corrupt", "oversized", "output"} {
		t.Run(failure, func(t *testing.T) {
			f := newStartFixture()
			wantPatches := 0
			if failure == "corrupt" || failure == "oversized" {
				f.responseKind = failure
				wantPatches = 1
			} else if failure == "output" {
				f.outputError = true
				wantPatches = 1
			} else {
				f.factoryFailure = failure
			}
			err, result, patches, _ := runStartWire(t, f, "-o=json")
			require.Error(t, err)
			require.Equal(t, 1, exitcode.FromError(err))
			require.Equal(t, wantPatches, patches)
			require.Empty(t, result.Kind)
			require.NotContains(t, err.Error(), "private secret")
		})
	}
}

func TestStartReviewAmbiguousIdentityClosedOutcome(t *testing.T) {
	f := newStartFixture()
	f.responseKind = "aliased kind"
	err, result, patches, _ := runStartWire(t, f, "-o=json")
	require.Equal(t, 1, exitcode.FromError(err))
	require.Equal(t, 1, patches)
	require.Empty(t, result.Kind)
	require.Contains(t, err.Error(), "API response is not bound to request; outcome unknown")
	require.Contains(t, err.Error(), "inspect migration status using the preview UUID")
}

// Catches missing source acquisition/planning and a second/unscoped patch.
func TestStartGeneratedRequestWire(t *testing.T) {
	for _, mode := range []string{"none", "client", "server"} {
		t.Run(mode, func(t *testing.T) {
			err, result, patches, preview := runStartWire(t, newStartFixture(), "-o=json", "--dry-run="+mode)
			require.NoError(t, err)
			require.Equal(t, "ActionResult", result.Kind)
			require.NotEmpty(t, result.RequestID)
			require.Equal(t, mode != "client", result.Accepted)
			require.Equal(t, mode == "none", result.Applied)
			want := 1
			if mode == "client" {
				want = 0
			}
			require.Equal(t, want, patches)
			require.Contains(t, preview, "node-a")
			require.NotContains(t, preview, "forged-node")
			require.Contains(t, result.FollowUp, "--component=engine")
		})
	}
}

func TestStartConflictingSourceNeverPatches(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*startFixture)
		code int
	}{
		{"paused", func(f *startFixture) { f.parent.Annotations[constants.PausedRolloutAnnotation] = "true" }, 3},
		{"IR paused", func(f *startFixture) { f.ir.Spec.Paused = true }, 3},
		{"Never", func(f *startFixture) {
			f.ir.Spec.Lifecycle = &v1beta1.LifecycleSpec{MigrationPolicy: &v1beta1.MigrationPolicy{Mode: v1beta1.MigrationPolicyModeNever}}
		}, 3},
		{"source stale", func(f *startFixture) { f.ir.Status.ObservedGeneration = 1 }, 3},
		{"owner replaced", func(f *startFixture) { f.ir.OwnerReferences[0].UID = "other" }, 3},
		{"not Ready", func(f *startFixture) { f.ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstancePending }, 3},
		{"node unavailable", func(f *startFixture) { f.pods[0].Spec.NodeName = "" }, 3},
		{"placement", func(f *startFixture) { f.parent.Finalizers = []string{"ome.io/placement"} }, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStartFixture()
			tc.edit(&f)
			err, result, patches, _ := runStartWire(t, f, "-o=json", "--dry-run=client")
			require.Error(t, err)
			require.Equal(t, tc.code, exitcode.FromError(err))
			require.Zero(t, patches)
			require.Empty(t, result.Kind)
		})
	}
}
