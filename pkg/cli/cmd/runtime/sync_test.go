package runtime

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
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	kfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
	kptr "k8s.io/utils/ptr"
	knapis "knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimerevision"
	"sigs.k8s.io/yaml"
)

// Missing registration would silently leave item38 inaccessible.
func TestRuntimeSyncRegisteredAlpha(t *testing.T) {
	c := NewCmd(&factory.Static{}, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{}})
	selected, _, err := c.Find([]string{"sync"})
	require.NoError(t, err)
	require.Equal(t, "sync", selected.Name())
	require.Contains(t, strings.ToLower(selected.Short), "alpha")
}

type syncFactory struct {
	factory.Static
	config *rest.Config
}

func (f *syncFactory) RESTConfig() (*rest.Config, error) { return f.config, nil }
func (f *syncFactory) RuntimeClientForAction(context.Context) (ctrlclient.Client, error) {
	return f.Runtime, nil
}

type ownedSyncFactory struct{ *syncFactory }

func (*ownedSyncFactory) OMEClient() (versioned.Interface, error) {
	panic("ordinary OME client must not be acquired")
}
func (*ownedSyncFactory) KubeClient() (kubernetes.Interface, error) {
	panic("ordinary Kubernetes client must not be acquired")
}
func (f *ownedSyncFactory) OMEClientForAction(context.Context) (versioned.Interface, error) {
	return f.OME, nil
}
func (f *ownedSyncFactory) KubeClientForAction(context.Context) (kubernetes.Interface, error) {
	return f.Kube, nil
}

func TestRuntimeSyncPrefersOwnedReadsWithoutAcquiringOrdinaryClients(t *testing.T) {
	f, _ := syncFixture(t)
	var out, errOut bytes.Buffer
	c := NewCmd(&ownedSyncFactory{f}, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
	c.SetArgs([]string{"sync", "service", "--yes", "--dry-run=client", "-o=json"})
	require.NoError(t, c.Execute())
	require.Contains(t, out.String(), `"accepted": false`)
	t.Logf("runtime sync CLI stdout:\n%s\nruntime sync CLI stderr:\n%s", out.String(), errOut.String())
}

func TestRuntimeSyncRefusesResponseWithoutRequestedToken(t *testing.T) {
	f, unchanged := syncFixture(t)
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		require.Equal(t, http.MethodPatch, r.Method)
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(unchanged))
	}))
	defer server.Close()
	f.config.Host = server.URL

	var out, errOut bytes.Buffer
	c := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
	c.SilenceErrors, c.SilenceUsage = true, true
	c.SetArgs([]string{"sync", "service", "--yes", "-o=json"})
	err := c.Execute()
	require.ErrorContains(t, err, "outcome unknown")
	require.Empty(t, out.String())
	require.Equal(t, 1, requests)
}

func syncFixture(t *testing.T) (*syncFactory, *v1beta1.InferenceService) {
	return syncFixtureWithRuntimeName(t, "runtime")
}
func syncFixtureWithRuntimeName(t *testing.T, runtimeName string) (*syncFactory, *v1beta1.InferenceService) {
	t.Helper()
	spec := v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "runner", Image: "old"}}}}
	raw, err := json.Marshal(&spec)
	require.NoError(t, err)
	_, hash, err := runtimerevision.Hash(&spec)
	require.NoError(t, err)
	name := runtimerevision.Name(runtimerevision.KindClusterServingRuntime, "", runtimeName, hash)
	rev := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ome", UID: types.UID("revision-uid"), ResourceVersion: "11", Labels: map[string]string{constants.RuntimeRevisionOfLabelKey: runtimeName, constants.RuntimeRevisionOfKindLabelKey: "ClusterServingRuntime", constants.RuntimeRevisionOfNamespaceLabelKey: "", constants.RuntimeRevisionHashLabelKey: hash}, Annotations: map[string]string{constants.RuntimeRevisionCreatedByKey: constants.RuntimeRevisionCreatedByOMEValue}}, Revision: 1, Data: kruntime.RawExtension{Raw: raw}}
	live := &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: runtimeName, UID: types.UID("runtime-uid"), ResourceVersion: "12", Generation: 2}, Spec: spec}
	live.Spec = *spec.DeepCopy()
	live.Spec.EngineConfig.Runner.Image = "new"
	v := &v1beta1.InferenceService{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"}, ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "team-a", UID: types.UID("service-uid"), ResourceVersion: "15", Generation: 3, Annotations: map[string]string{"unrelated": "PRIVATE_UNRELATED"}}, Spec: v1beta1.InferenceServiceSpec{Runtime: &v1beta1.ServingRuntimeRef{Name: runtimeName, AutoSync: kptr.To(false)}, Engine: &v1beta1.EngineSpec{}}}
	v.Status.PinnedRevisionName = name
	v.Status.Conditions = duckv1.Conditions{{Type: knapis.ConditionType(constants.RuntimeDriftedConditionType), Status: corev1.ConditionTrue, Reason: "RevisionMismatch"}}
	scheme := kruntime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	f := &syncFactory{Static: factory.Static{OME: omefake.NewSimpleClientset(v), Kube: kfake.NewSimpleClientset(rev), Runtime: ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(live).Build(), NS: "team-a", Context: "dev-fra"}, config: &rest.Config{Host: "http://127.0.0.1:1"}}
	return f, v
}

func TestRuntimeSyncRefusesUnrepresentableWriterLabelBeforeHistory(t *testing.T) {
	runtimeName := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	f, _ := syncFixtureWithRuntimeName(t, runtimeName)
	var out, errOut bytes.Buffer
	c := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
	c.SetArgs([]string{"sync", "service", "--yes", "--dry-run=client", "-o=json"})
	require.ErrorContains(t, c.Execute(), "managed runtime sync safety")
	require.Empty(t, out.String())
	require.Empty(t, f.Kube.(*kfake.Clientset).Actions())
}

func TestRuntimeSyncLongExactContextWrapsWithoutTruncation(t *testing.T) {
	f, _ := syncFixture(t)
	f.Context = strings.Repeat("c", 256)
	var out, errOut bytes.Buffer
	c := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
	c.SetArgs([]string{"sync", "service", "--yes", "--dry-run=client", "-o=json"})
	require.NoError(t, c.Execute())
	for _, line := range strings.Split(errOut.String(), "\n") {
		require.LessOrEqual(t, len(line), 80)
	}
	require.Contains(t, out.String(), f.Context)
	var shown string
	for _, line := range strings.Split(errOut.String(), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && (fields[0] == "Context" || fields[0] == "(continued)") {
			shown += fields[1]
		}
	}
	require.Equal(t, f.Context, shown)
}

func TestRuntimeSyncNativeStableSnapshotAndSafetyRecheck(t *testing.T) {
	for _, scenario := range []string{"stable", "held during confirmation", "IR resourceVersion changed", "missing"} {
		t.Run(scenario, func(t *testing.T) {
			f, v := syncFixture(t)
			mode := constants.OMENative
			v.Spec.DeploymentMode = &mode
			ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: v.Name + "-engine", Namespace: v.Namespace, UID: "ir-uid", ResourceVersion: "ir-rv", Generation: 1, Labels: map[string]string{constants.InferenceServiceLabel: v.Name}, Annotations: map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: "3"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: v.Name, UID: v.UID, Controller: kptr.To(true)}}}, Spec: v1beta1.InferenceReplicaSpec{ParentRef: v1beta1.ParentReference{Name: v.Name}, Component: v1beta1.EngineComponent}, Status: v1beta1.InferenceReplicaStatus{ObservedGeneration: 1}}
			client := omefake.NewSimpleClientset(v, ir)
			reads := 0
			client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, kruntime.Object, error) {
				reads++
				list := &v1beta1.InferenceReplicaList{Items: []v1beta1.InferenceReplica{*ir.DeepCopy()}}
				if scenario == "missing" {
					list.Items = nil
				}
				if reads == 2 {
					if scenario == "IR resourceVersion changed" {
						list.Items[0].ResourceVersion = "changed"
					}
					if scenario == "held during confirmation" {
						list.Items[0].Status.RetryBlocks = []v1beta1.RetryBlock{{TargetRevision: ir.Name + "-aaaaaaaa", State: v1beta1.RetryBlockHeld}}
					}
				}
				return true, list, nil
			})
			f.OME = client
			var out, errOut bytes.Buffer
			c := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
			c.SilenceErrors, c.SilenceUsage = true, true
			c.SetArgs([]string{"sync", "service", "--yes", "--dry-run=client", "-o=json"})
			err := c.Execute()
			if scenario == "stable" {
				require.NoError(t, err)
				require.Equal(t, 2, reads)
				return
			}
			require.Error(t, err)
			require.Empty(t, out.String())
			if scenario != "missing" {
				var stale *exitcode.PreconditionError
				require.ErrorAs(t, err, &stale)
				require.Equal(t, 2, reads)
			} else {
				require.Equal(t, 1, reads)
			}
		})
	}
}

func TestRuntimeSyncWireErrorClassificationNoReplay(t *testing.T) {
	for _, scenario := range []string{"409", "native 422", "admission 422", "500", "cancel before acquisition"} {
		t.Run(scenario, func(t *testing.T) {
			f, _ := syncFixture(t)
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				status := metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonInvalid, Code: 422, Message: "the server rejected our request due to an error in our request", Details: &metav1.StatusDetails{}}
				switch scenario {
				case "409":
					status.Code, status.Reason = 409, metav1.StatusReasonConflict
				case "admission 422":
					status.Message = "PRIVATE_ADMISSION"
					status.Details.Name = "service"
				case "500":
					status.Code, status.Reason, status.Message = 500, metav1.StatusReasonInternalError, "PRIVATE_INTERNAL"
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(int(status.Code))
				_ = json.NewEncoder(w).Encode(status)
			}))
			defer server.Close()
			f.config.Host = server.URL
			var out, errOut bytes.Buffer
			c := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
			c.SilenceErrors, c.SilenceUsage = true, true
			c.SetArgs([]string{"sync", "service", "--yes", "-o=json"})
			ctx := context.Background()
			if scenario == "cancel before acquisition" {
				var stop context.CancelFunc
				ctx, stop = context.WithCancel(ctx)
				stop()
			}
			err := c.ExecuteContext(ctx)
			require.Error(t, err)
			require.Empty(t, out.String())
			require.NotContains(t, err.Error(), "PRIVATE")
			if scenario == "cancel before acquisition" {
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, requests)
				return
			}
			require.Equal(t, 1, requests)
			var stale *exitcode.PreconditionError
			require.Equal(t, scenario == "409" || scenario == "native 422", errors.As(err, &stale))
			if scenario == "500" {
				require.ErrorContains(t, err, "outcome unknown")
			}
		})
	}
}

// Refusing every request, requiring parent global status freshness, changing
// unrelated fields, or claiming dry-run application would break this behavior.
func TestRuntimeSyncEligibleFormatsAndAnnotationOnlyWire(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		for _, mode := range []string{"client", "server", "none"} {
			t.Run(format+"/"+mode, func(t *testing.T) {
				f, v := syncFixture(t)
				requests := 0
				before, err := json.Marshal(v)
				require.NoError(t, err)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests++
					require.Equal(t, "PATCH", r.Method)
					require.Equal(t, "/apis/ome.io/v1beta1/namespaces/team-a/inferenceservices/service", r.URL.Path)
					require.Equal(t, "application/json-patch+json", r.Header.Get("Content-Type"))
					if mode == "server" {
						require.Equal(t, "All", r.URL.Query().Get("dryRun"))
					} else {
						require.Empty(t, r.URL.Query().Get("dryRun"))
					}
					body, e := io.ReadAll(r.Body)
					require.NoError(t, e)
					patch, e := jsonpatch.DecodePatch(body)
					require.NoError(t, e)
					after, e := patch.Apply(before)
					require.NoError(t, e)
					var got v1beta1.InferenceService
					require.NoError(t, json.Unmarshal(after, &got))
					token := got.Annotations[constants.RuntimeSyncAnnotationKey]
					require.NotEmpty(t, token)
					delete(got.Annotations, constants.RuntimeSyncAnnotationKey)
					check, e := json.Marshal(&got)
					require.NoError(t, e)
					require.JSONEq(t, string(before), string(check))
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write(after)
				}))
				defer server.Close()
				f.config.Host = server.URL
				var out, errOut bytes.Buffer
				c := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
				c.SilenceErrors = true
				c.SilenceUsage = true
				c.SetArgs([]string{"sync", "service", "--yes", "--dry-run", mode, "-o", format})
				require.NoError(t, c.Execute())
				if mode == "client" {
					require.Zero(t, requests)
				} else {
					require.Equal(t, 1, requests)
				}
				require.Contains(t, errOut.String(), "not locked")
				require.Contains(t, errOut.String(), "Unverifiable")
				require.NotContains(t, errOut.String(), "PRIVATE_UNRELATED")
				for _, line := range strings.Split(errOut.String(), "\n") {
					require.LessOrEqual(t, len(line), 80)
				}
				if format == "json" || format == "yaml" {
					var result reportv1alpha1.ActionResult
					data := out.Bytes()
					if format == "yaml" {
						data, err = yaml.YAMLToJSON(data)
						require.NoError(t, err)
					}
					d := json.NewDecoder(bytes.NewReader(data))
					require.NoError(t, d.Decode(&result))
					require.ErrorIs(t, d.Decode(new(any)), io.EOF)
					require.Equal(t, "runtime sync", result.Action)
					require.Equal(t, mode != "client", result.Accepted)
					require.Equal(t, mode == "none", result.Applied)
					require.NotEmpty(t, result.RequestID)
					require.Contains(t, result.FollowUp, "runtime effective")
				}
			})
		}
	}
}

func TestRuntimeSyncUnsafeParserNeverAcquiresFactory(t *testing.T) {
	for _, args := range [][]string{{}, {"service", "extra"}, {"Bad_PRIVATE"}, {"service", "-o", "PRIVATE"}, {"service", "--dry-run", "PRIVATE"}, {"service", "--token", "PRIVATE"}} {
		f := &validationFactory{}
		var out, errOut bytes.Buffer
		c := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
		c.SilenceErrors = true
		c.SilenceUsage = true
		c.SetArgs(append([]string{"sync"}, args...))
		err := c.Execute()
		require.Error(t, err)
		require.NotContains(t, err.Error(), "PRIVATE")
		require.Zero(t, f.namespaceCalls)
		require.Empty(t, out.String())
	}
}

func TestRuntimeSyncRechecksOnceAndRefusesChangedSnapshot(t *testing.T) {
	for _, scenario := range []string{"parent RV", "status generation", "status token", "pin", "source RV", "source digest"} {
		t.Run(scenario, func(t *testing.T) {
			f, v := syncFixture(t)
			reads := 0
			client := f.OME.(*omefake.Clientset)
			client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, kruntime.Object, error) {
				reads++
				copy := v.DeepCopy()
				if reads == 2 {
					switch scenario {
					case "parent RV":
						copy.ResourceVersion = "changed"
					case "status generation":
						copy.Status.ObservedGeneration = 4
					case "status token":
						copy.Status.LastRuntimeSyncToken = "PRIVATE_TOKEN"
					case "pin":
						copy.Status.PinnedRevisionName = "different"
					default:
						live := &v1beta1.ClusterServingRuntime{}
						require.NoError(t, f.Runtime.Get(context.Background(), ctrlclient.ObjectKey{Name: "runtime"}, live))
						if scenario == "source digest" {
							live.Spec.EngineConfig.Runner.Image = "different"
						} else {
							live.Annotations = map[string]string{"source": "changed"}
						}
						require.NoError(t, f.Runtime.Update(context.Background(), live))
					}
				}
				return true, copy, nil
			})
			var out, errOut bytes.Buffer
			c := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
			c.SilenceErrors = true
			c.SilenceUsage = true
			c.SetArgs([]string{"sync", "service", "--yes", "--dry-run=client", "-o", "json"})
			err := c.Execute()
			require.Error(t, err)
			require.Equal(t, 3, exitcode.FromError(err))
			require.Equal(t, 2, reads)
			require.Empty(t, out.String())
			require.NotContains(t, err.Error(), "PRIVATE")
		})
	}
}

type syncFailWriter struct{}

func (syncFailWriter) Write([]byte) (int, error) { return 0, fmt.Errorf("PRIVATE_WRITER") }

func TestRuntimeSyncConfirmationAndPreviewFailuresNeverSend(t *testing.T) {
	for _, scenario := range []string{"noninteractive", "preview failure"} {
		t.Run(scenario, func(t *testing.T) {
			f, _ := syncFixture(t)
			var out, errOut bytes.Buffer
			streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut}
			args := []string{"sync", "service", "--dry-run=client"}
			if scenario == "preview failure" {
				streams.ErrOut = syncFailWriter{}
				args = append(args, "--yes")
			}
			c := NewCmd(f, streams)
			c.SilenceErrors = true
			c.SilenceUsage = true
			c.SetArgs(args)
			err := c.Execute()
			require.Error(t, err)
			require.NotContains(t, err.Error(), "PRIVATE")
			require.Empty(t, out.String())
			require.Len(t, f.OME.(*omefake.Clientset).Actions(), 1)
		})
	}
}

func TestRuntimeSyncUnboundResponseNeverClaimsSuccessOrReplays(t *testing.T) {
	for _, scenario := range []string{"wrong UID", "aliased identity", "oversized", "stdout failure", "cancel after send"} {
		t.Run(scenario, func(t *testing.T) {
			f, v := syncFixture(t)
			requests := 0
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				require.Equal(t, "PATCH", r.Method)
				w.Header().Set("Content-Type", "application/json")
				switch scenario {
				case "wrong UID":
					_, _ = io.WriteString(w, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"service","namespace":"team-a","uid":"wrong","resourceVersion":"16"}}`)
				case "aliased identity":
					_, _ = io.WriteString(w, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"service","Name":"PRIVATE_ALIAS","namespace":"team-a","uid":"service-uid","resourceVersion":"16"}}`)
				case "oversized":
					_, _ = io.WriteString(w, strings.Repeat("PRIVATE_BODY", 100000))
				default:
					if scenario == "cancel after send" {
						cancel()
					}
					_ = json.NewEncoder(w).Encode(v)
				}
			}))
			defer server.Close()
			f.config.Host = server.URL
			var out, errOut bytes.Buffer
			streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut}
			if scenario == "stdout failure" {
				streams.Out = syncFailWriter{}
			}
			c := NewCmd(f, streams)
			c.SilenceErrors = true
			c.SilenceUsage = true
			c.SetContext(ctx)
			c.SetArgs([]string{"sync", "service", "--yes", "-o", "json"})
			err := c.Execute()
			require.Error(t, err)
			require.NotContains(t, err.Error(), "PRIVATE")
			require.Contains(t, err.Error(), "outcome")
			require.Equal(t, 1, requests)
			require.Empty(t, out.String())
		})
	}
}
