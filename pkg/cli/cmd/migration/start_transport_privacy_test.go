package migration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

// Return the original configuration, as the production factory does. The
// ordinary startFactory's CopyConfig would itself replace ExecProvider.Config.
type ownedStartConfigFactory struct {
	factory.Static
	config *rest.Config
	calls  int
}

func (f *ownedStartConfigFactory) RESTConfig() (*rest.Config, error) {
	f.calls++
	return f.config, nil
}

func TestStartPreservesCallerExecConfiguration(t *testing.T) {
	for _, mode := range []string{"client", "server", "none"} {
		t.Run(mode, func(t *testing.T) {
			fixture := newStartFixture()
			patches := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
				patches++
				require.Equal(t, "PATCH", q.Method)
				require.Equal(t, "/apis/ome.io/v1beta1/namespaces/prod/inferenceservices/chat", q.URL.Path)
				require.Equal(t, mode == "server", q.URL.Query().Get("dryRun") == "All")
				require.Equal(t, "Bearer fixture-token", q.Header.Get("Authorization"))
				w.Header().Set("Content-Type", "application/json")
				response := fixture.parent.DeepCopy()
				response.ResourceVersion = "43"
				require.NoError(t, json.NewEncoder(w).Encode(response))
			}))
			defer server.Close()
			original := &runtime.Unknown{Raw: []byte(`{"private":"PRIVATE_SENTINEL"}`)}
			provider := &clientcmdapi.ExecConfig{Command: "never-executed", APIVersion: "client.authentication.k8s.io/v1beta1", InteractiveMode: clientcmdapi.NeverExecInteractiveMode, Config: original}
			// An explicit token is supported by client-go and avoids invoking the
			// credential plugin while exercising actual bounded mutation transport.
			config := &rest.Config{Host: server.URL, Timeout: 30 * time.Second, BearerToken: "fixture-token", ExecProvider: provider}
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			rt := &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: "prod", UID: "uid-runtime", ResourceVersion: "99", Generation: 1}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "busybox"}}}}}
			f := &ownedStartConfigFactory{Static: factory.Static{
				OME:     omefake.NewSimpleClientset(fixture.parent, fixture.ir),
				Kube:    kubefake.NewClientset(fixture.cr, &fixture.pods[0]),
				Runtime: clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(rt).Build(),
				NS:      "prod", Context: "moirai",
			}, config: config}
			var out, stderr bytes.Buffer
			cmd := NewCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs([]string{"start", "chat", "--component=engine", "--instance=3", "--yes", "--dry-run=" + mode, "-o=json"})
			require.NoError(t, cmd.Execute())
			var result reportv1alpha1.ActionResult
			require.NoError(t, json.Unmarshal(out.Bytes(), &result))
			wantMutations := 1
			if mode == "client" {
				wantMutations = 0
			}
			require.Equal(t, wantMutations, patches)
			require.Equal(t, wantMutations, f.calls)
			require.Equal(t, mode != "client", result.Accepted)
			require.Equal(t, mode == "none", result.Applied)
			require.Same(t, config, f.config)
			require.Same(t, provider, config.ExecProvider)
			require.Same(t, original, provider.Config, "actual migration runner replaced caller-owned exec configuration")
			require.Equal(t, `{"private":"PRIVATE_SENTINEL"}`, string(original.Raw))
			require.Equal(t, 30*time.Second, config.Timeout)
			require.NotContains(t, out.String()+stderr.String(), "PRIVATE_SENTINEL")
		})
	}
}
