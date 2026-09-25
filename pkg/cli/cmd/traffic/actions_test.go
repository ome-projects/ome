package traffic

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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
	"sigs.k8s.io/yaml"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/mutate"
	"sigs.k8s.io/ome/pkg/cli/printers"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

const testTrafficActionResponseLimit = 1024 * 1024

type trafficActionFactory struct {
	factory.Static
	config      *rest.Config
	omeErr      error
	contextErr  error
	omeCalls    atomic.Int32
	configCalls atomic.Int32
}

func (f *trafficActionFactory) OMEClient() (versioned.Interface, error) {
	f.omeCalls.Add(1)
	if f.omeErr != nil {
		return nil, f.omeErr
	}
	return f.Static.OMEClient()
}

func (f *trafficActionFactory) RESTConfig() (*rest.Config, error) {
	f.configCalls.Add(1)
	if f.config == nil {
		return nil, errors.New("PRIVATE_CONFIG_ERROR")
	}
	return f.config, nil
}

func (f *trafficActionFactory) ContextName() (string, error) {
	if f.contextErr != nil {
		return "", f.contextErr
	}
	return f.Static.ContextName()
}

func TestTrafficActionsHelpContract(t *testing.T) {
	cmd := NewCmd(&trafficActionFactory{}, genericiooptions.IOStreams{})
	for _, action := range []string{"drain", "undrain"} {
		sub, _, err := cmd.Find([]string{action})
		require.NoError(t, err)
		require.Equal(t, action+" INFERENCESERVICE", sub.Use)
		for _, flag := range []string{"id", "yes", "dry-run", "output"} {
			require.NotNil(t, sub.Flags().Lookup(flag), action+" --"+flag)
		}
		assert.Contains(t, sub.Long, "45-second")
		assert.Contains(t, sub.Long, "10 seconds")
		assert.Contains(t, sub.Long, "not TrafficMap convergence")
	}
	drain, _, err := cmd.Find([]string{"drain"})
	require.NoError(t, err)
	require.NotNil(t, drain.Flags().Lookup("workload-cluster"))
	require.Nil(t, drain.Flags().Lookup("cluster"), "the API cluster flag must not be shadowed")
	require.NotNil(t, drain.Flags().Lookup("reason"))
	undrain, _, err := cmd.Find([]string{"undrain"})
	require.NoError(t, err)
	require.Nil(t, undrain.Flags().Lookup("cluster"))
	require.Nil(t, undrain.Flags().Lookup("workload-cluster"))
	require.Nil(t, undrain.Flags().Lookup("reason"))
}

func TestTrafficActionClientDryRunUsesOneExactReadAndNoPatch(t *testing.T) {
	for _, tc := range []struct {
		action      string
		annotations map[string]string
		args        []string
	}{
		{action: "drain", args: []string{"drain", "chat", "--workload-cluster=worker-a", "--id=maintenance-a", "--reason=planned maintenance"}},
		{action: "undrain", annotations: map[string]string{constants.TrafficDrainAnnotation: `{"maintenance-a":{"cluster":"worker-a","reason":"planned maintenance"}}`}, args: []string{"undrain", "chat", "--id=maintenance-a"}},
	} {
		t.Run(tc.action, func(t *testing.T) {
			service := trafficActionService(tc.annotations)
			client := omefake.NewSimpleClientset(service)
			var gets atomic.Int32
			client.PrependReactor("get", "inferenceservices", func(action clienttesting.Action) (bool, runtime.Object, error) {
				gets.Add(1)
				return false, nil, nil
			})
			f := &trafficActionFactory{Static: factory.Static{OME: client, NS: "prod", Context: "moirai"}}
			var out, stderr bytes.Buffer
			cmd := NewCmd(f, genericiooptions.IOStreams{In: bytes.NewBufferString("unused"), Out: &out, ErrOut: &stderr})
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs(append(tc.args, "--yes", "--dry-run=client", "-o=json"))
			require.NoError(t, cmd.Execute())
			require.Equal(t, int32(1), gets.Load())
			require.Equal(t, int32(1), f.omeCalls.Load())
			require.Zero(t, f.configCalls.Load(), "client dry-run must not construct a mutation transport")
			var result reportv1alpha1.ActionResult
			require.NoError(t, json.Unmarshal(out.Bytes(), &result))
			assert.Equal(t, "traffic "+tc.action, result.Action)
			require.NotNil(t, result.Traffic)
			assert.Equal(t, "maintenance-a", result.Traffic.OverrideID)
			assert.Equal(t, "worker-a", result.Traffic.Cluster)
			assert.Empty(t, result.RequestID, "durable override IDs are not request UUIDs")
			assert.False(t, result.Accepted)
			assert.False(t, result.Applied)
			assert.Contains(t, result.Message, "no patch sent")
			assert.Contains(t, result.FollowUp, "kubectl ome traffic status chat")
			assert.Contains(t, stderr.String(), "ALPHA guarded traffic action")
			assert.Contains(t, stderr.String(), "maintenance-a")
			for _, line := range strings.Split(strings.TrimSuffix(stderr.String(), "\n"), "\n") {
				require.LessOrEqual(t, printers.CellDisplayWidth(line), 80, line)
			}
		})
	}
}

func TestTrafficActionSendsExactPatchForApplyAndServerDryRun(t *testing.T) {
	for _, mode := range []string{"none", "server"} {
		for _, action := range []string{"drain", "undrain"} {
			t.Run(mode+"/"+action, func(t *testing.T) {
				annotations := map[string]string{"example.com/keep": "yes"}
				args := []string{action, "chat", "--id=maintenance-a", "--yes", "--dry-run=" + mode, "-o=json"}
				if action == "drain" {
					args = append(args, "--workload-cluster=worker-a", "--reason=planned maintenance")
				} else {
					annotations[constants.TrafficDrainAnnotation] = `{"maintenance-a":{"cluster":"worker-a","reason":"planned maintenance"},"keep":{"cluster":"worker-b","reason":"still active"}}`
				}
				service := trafficActionService(annotations)
				var requests atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests.Add(1)
					require.Equal(t, http.MethodPatch, r.Method)
					require.Equal(t, "/apis/ome.io/v1beta1/namespaces/prod/inferenceservices/chat", r.URL.Path)
					require.Equal(t, mode == "server", r.URL.Query().Get("dryRun") == "All")
					require.Equal(t, "application/json-patch+json", r.Header.Get("Content-Type"))
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					assert.Contains(t, string(body), `"path":"/metadata/uid","value":"uid-chat"`)
					assert.Contains(t, string(body), `"path":"/metadata/resourceVersion","value":"42"`)
					assert.Contains(t, string(body), `"path":"/metadata/annotations/ome.io~1traffic-drain"`)
					assert.NotContains(t, string(body), "example.com/keep", "unrelated annotations must not be rewritten")
					w.Header().Set("Content-Type", "application/json")
					response := service.DeepCopy()
					response.ResourceVersion = "43"
					require.NoError(t, json.NewEncoder(w).Encode(response))
				}))
				defer server.Close()
				f := &trafficActionFactory{
					Static: factory.Static{OME: omefake.NewSimpleClientset(service), NS: "prod", Context: "moirai"},
					config: &rest.Config{Host: server.URL, Timeout: 30 * time.Second},
				}
				var out, stderr bytes.Buffer
				cmd := NewCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
				cmd.SilenceErrors, cmd.SilenceUsage = true, true
				cmd.SetArgs(args)
				require.NoError(t, cmd.Execute())
				require.Equal(t, int32(1), requests.Load())
				var result reportv1alpha1.ActionResult
				require.NoError(t, json.Unmarshal(out.Bytes(), &result))
				assert.True(t, result.Accepted)
				assert.Equal(t, mode == "none", result.Applied)
				assert.Equal(t, reportv1alpha1.DryRunMode(mode), result.DryRun)
				if mode == "server" {
					assert.Contains(t, result.Message, "no changes persisted")
				} else {
					assert.Contains(t, result.Message, "not TrafficMap convergence")
				}
			})
		}
	}
}

func TestTrafficActionRejectsInvalidInputBeforeAPI(t *testing.T) {
	private := "PRIVATE_SENTINEL"
	tests := [][]string{
		{"drain"},
		{"drain", "chat", "extra", "--id=safe", "--workload-cluster=worker-a", "--reason=safe"},
		{"drain", "Bad_Name", "--id=safe", "--workload-cluster=worker-a", "--reason=safe"},
		{"drain", "chat", "--id=Bad_ID", "--workload-cluster=worker-a", "--reason=safe"},
		{"drain", "chat", "--id=safe", "--workload-cluster=BAD_CLUSTER", "--reason=safe"},
		{"drain", "chat", "--id=safe", "--workload-cluster=worker-a", "--reason=line\nbreak"},
		{"drain", "chat", "--id=safe", "--workload-cluster=worker-a", "--reason=sk-123456789012345678901234"},
		{"drain", "chat", "--id=safe", "--workload-cluster=worker-a", "--reason=safe", "--dry-run=bad"},
		{"drain", "chat", "--id=safe", "--workload-cluster=worker-a", "--reason=safe", "-o=bad"},
		{"drain", "chat", "--id=safe", "--workload-cluster=worker-a", "--reason=safe", "--yes=" + private},
		{"undrain", "chat"},
		{"undrain", "chat", "--id=safe", "--dry-run=" + private},
		{"undrain", "chat", "--id=safe", "--unknown=" + private},
	}
	for _, args := range tests {
		t.Run(strings.Join(args, "/"), func(t *testing.T) {
			f := &trafficActionFactory{Static: factory.Static{OME: omefake.NewSimpleClientset(), NS: "prod", Context: "moirai"}}
			var out, stderr bytes.Buffer
			cmd := NewCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs(args)
			err := cmd.Execute()
			require.Error(t, err)
			require.Zero(t, f.omeCalls.Load(), "invalid input must not construct or call an API client")
			require.Zero(t, f.configCalls.Load())
			require.Empty(t, out.String())
			require.Empty(t, stderr.String())
			require.NotContains(t, err.Error(), private)
		})
	}
}

func TestTrafficActionRefusesUnsafeExistingStateBeforePreview(t *testing.T) {
	tests := map[string]string{
		"malformed":       `{`,
		"duplicate":       `{"same":{"cluster":"worker-a","reason":"one"},"same":{"cluster":"worker-b","reason":"two"}}`,
		"unknown field":   `{"same":{"cluster":"worker-a","reason":"one","weight":"0"}}`,
		"unsafe existing": `{"BAD ID":{"cluster":"worker-a","reason":"one"}}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			service := trafficActionService(map[string]string{constants.TrafficDrainAnnotation: raw})
			f := &trafficActionFactory{Static: factory.Static{OME: omefake.NewSimpleClientset(service), NS: "prod", Context: "moirai"}}
			var out, stderr bytes.Buffer
			cmd := NewCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs([]string{"undrain", "chat", "--id=same", "--yes", "--dry-run=client"})
			require.Error(t, cmd.Execute())
			require.Empty(t, out.String())
			require.Empty(t, stderr.String())
		})
	}
}

func TestTrafficActionRefusesIneligibleServiceBeforePreviewOrPatch(t *testing.T) {
	service := trafficActionService(nil)
	service.Spec.Placement = &omev1beta1.PlacementSpec{Mode: omev1beta1.PlacementModeAll}
	client := omefake.NewSimpleClientset(service)
	var gets atomic.Int32
	client.PrependReactor("get", "inferenceservices", func(action clienttesting.Action) (bool, runtime.Object, error) {
		gets.Add(1)
		return false, nil, nil
	})
	f := &trafficActionFactory{Static: factory.Static{OME: client, NS: "prod", Context: "moirai"}}
	var out, stderr bytes.Buffer
	cmd := NewCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetArgs([]string{"drain", "chat", "--id=safe", "--workload-cluster=worker-a", "--reason=maintenance", "--yes"})
	err := cmd.Execute()
	require.ErrorIs(t, err, mutate.ErrTrafficDrainIneligible)
	assert.Equal(t, int32(1), gets.Load(), "eligibility is proven from one exact target read")
	assert.Zero(t, f.configCalls.Load(), "an ineligible target must not construct a patch transport")
	assert.Empty(t, out.String())
	assert.Empty(t, stderr.String(), "refusal happens before preview or confirmation")
}

func TestTrafficActionRequiresTTYOrYes(t *testing.T) {
	service := trafficActionService(nil)
	f := &trafficActionFactory{Static: factory.Static{OME: omefake.NewSimpleClientset(service), NS: "prod", Context: "moirai"}}
	var out, stderr bytes.Buffer
	cmd := NewCmd(f, genericiooptions.IOStreams{In: bytes.NewBufferString("yes\n"), Out: &out, ErrOut: &stderr})
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetArgs([]string{"drain", "chat", "--id=safe", "--workload-cluster=worker-a", "--reason=maintenance", "--dry-run=client"})
	err := cmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "noninteractive input requires --yes")
	assert.Empty(t, out.String())
	assert.Contains(t, stderr.String(), "ALPHA guarded traffic action")
}

func TestTrafficActionCancellationAndReadTimeout(t *testing.T) {
	for _, tc := range []struct {
		name    string
		context func() (context.Context, context.CancelFunc)
		want    error
	}{
		{name: "canceled", context: func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx, func() {}
		}, want: context.Canceled},
		{name: "deadline", context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), time.Nanosecond)
		}, want: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := tc.context()
			defer cancel()
			f := &trafficActionFactory{Static: factory.Static{OME: omefake.NewSimpleClientset(trafficActionService(nil)), NS: "prod", Context: "moirai"}}
			cmd := NewCmd(f, genericiooptions.IOStreams{})
			cmd.SetContext(ctx)
			cmd.SetArgs([]string{"drain", "chat", "--id=safe", "--workload-cluster=worker-a", "--reason=maintenance", "--yes", "--dry-run=client"})
			err := cmd.Execute()
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestTrafficActionCancelsAnInFlightTargetRead(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	config := &rest.Config{Host: server.URL}
	client, err := versioned.NewForConfig(config)
	require.NoError(t, err)
	f := &trafficActionFactory{Static: factory.Static{OME: client, NS: "prod", Context: "moirai"}}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var out, stderr bytes.Buffer
	cmd := NewCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"drain", "chat", "--id=safe", "--workload-cluster=worker-a", "--reason=maintenance", "--yes", "--dry-run=client"})
	err = cmd.Execute()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("target GET did not reach the scripted API")
	}
	assert.Empty(t, out.String())
	assert.Empty(t, stderr.String(), "timeout before preview must not emit a partial action")
}

func TestTrafficActionSanitizesPatchFailuresAndUnknownOutcomes(t *testing.T) {
	service := trafficActionService(nil)
	tests := []struct {
		name  string
		serve func(http.ResponseWriter, *http.Request)
		match string
	}{
		{name: "CAS conflict", serve: func(w http.ResponseWriter, _ *http.Request) {
			err := apierrors.NewConflict(schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}, "chat", errors.New("PRIVATE_CONFLICT"))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(err.Status())
		}, match: "refresh traffic status"},
		{name: "private admission", serve: func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "PRIVATE_ADMISSION_SECRET", http.StatusForbidden)
		}, match: "required Kubernetes API request failed"},
		{name: "wrong identity", serve: func(w http.ResponseWriter, _ *http.Request) {
			wrong := service.DeepCopy()
			wrong.Name = "different"
			wrong.ResourceVersion = "43"
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(wrong)
		}, match: "outcome unknown"},
		{name: "oversized response", serve: func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, strings.Repeat("x", testTrafficActionResponseLimit+1))
		}, match: "outcome unknown"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(tc.serve))
			defer server.Close()
			f := &trafficActionFactory{
				Static: factory.Static{OME: omefake.NewSimpleClientset(service), NS: "prod", Context: "moirai"},
				config: &rest.Config{Host: server.URL},
			}
			var out, stderr bytes.Buffer
			cmd := NewCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs([]string{"drain", "chat", "--id=safe", "--workload-cluster=worker-a", "--reason=maintenance", "--yes", "-o=json"})
			err := cmd.Execute()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.match)
			assert.NotContains(t, err.Error(), "PRIVATE")
			assert.Empty(t, out.String())
		})
	}
}

func TestTrafficActionOutputFormats(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			f := &trafficActionFactory{Static: factory.Static{OME: omefake.NewSimpleClientset(trafficActionService(nil)), NS: "prod", Context: "moirai"}}
			var out bytes.Buffer
			cmd := NewCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: io.Discard})
			cmd.SetArgs([]string{"drain", "chat", "--id=safe", "--workload-cluster=worker-a", "--reason=maintenance", "--yes", "--dry-run=client", "-o=" + format})
			require.NoError(t, cmd.Execute())
			assert.Contains(t, out.String(), "traffic drain")
			if format == "json" || format == "yaml" {
				assert.Contains(t, out.String(), "ActionResult")
				var result reportv1alpha1.ActionResult
				if format == "json" {
					require.NoError(t, json.Unmarshal(out.Bytes(), &result))
				} else {
					require.NoError(t, yaml.Unmarshal(out.Bytes(), &result))
				}
				require.NotNil(t, result.Traffic)
				assert.Equal(t, "safe", result.Traffic.OverrideID)
				assert.Equal(t, "worker-a", result.Traffic.Cluster)
			}
		})
	}
}

func trafficActionService(annotations map[string]string) *omev1beta1.InferenceService {
	return &omev1beta1.InferenceService{
		TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"), ResourceVersion: "42", Generation: 2,
			Annotations: annotations,
		},
		Spec: omev1beta1.InferenceServiceSpec{Placement: &omev1beta1.PlacementSpec{
			Mode: omev1beta1.PlacementModeAll, Requirements: "accelerator=test",
		}},
	}
}
