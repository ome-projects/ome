package factory

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

type actionReadsCapability interface {
	OMEClientForAction(context.Context) (versioned.Interface, error)
	KubeClientForAction(context.Context) (kubernetes.Interface, error)
}

type privateWarningSentinel struct{ calls atomic.Int32 }

func (s *privateWarningSentinel) HandleWarningHeader(int, string, string) { s.calls.Add(1) }
func (s *privateWarningSentinel) HandleWarningHeaderWithContext(context.Context, int, string, string) {
	s.calls.Add(1)
}

func TestActionReadClientsSuppressInheritedWarningsWithoutChangingOrdinaryCache(t *testing.T) {
	for _, contextual := range []bool{false, true} {
		t.Run(fmt.Sprint(contextual), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Warning", `299 private-agent "PRIVATE_WARNING_CONTENT"`)
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/apis/apps/v1/namespaces/team-a/controllerrevisions" {
					_, _ = w.Write([]byte(`{"apiVersion":"apps/v1","kind":"ControllerRevisionList","metadata":{},"items":[]}`))
				} else {
					_, _ = w.Write([]byte(`{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"service","namespace":"team-a"}}`))
				}
			}))
			defer server.Close()
			sentinel := &privateWarningSentinel{}
			config := &rest.Config{Host: server.URL, Timeout: 2 * time.Second, WarningHandler: sentinel}
			if contextual {
				config.WarningHandlerWithContext = sentinel
			}
			f := &defaultFactory{rest: config}
			owned, ok := any(f).(actionReadsCapability)
			require.True(t, ok, "production actions need optional uncached owned typed clients")
			ome, err := owned.OMEClientForAction(context.Background())
			require.NoError(t, err)
			_, err = ome.OmeV1beta1().InferenceServices("team-a").Get(context.Background(), "service", metav1.GetOptions{})
			require.NoError(t, err)
			kube, err := owned.KubeClientForAction(context.Background())
			require.NoError(t, err)
			_, err = kube.AppsV1().ControllerRevisions("team-a").List(context.Background(), metav1.ListOptions{})
			require.NoError(t, err)
			require.Zero(t, sentinel.calls.Load(), "hostile GET/LIST Warning text must not reach either inherited handler")
			require.Same(t, sentinel, config.WarningHandler)
			if contextual {
				require.Same(t, sentinel, config.WarningHandlerWithContext)
			}
			require.Equal(t, 2*time.Second, config.Timeout)
			require.Nil(t, f.ome)
			require.Nil(t, f.kube)
			ordinary, err := f.OMEClient()
			require.NoError(t, err)
			_, err = ordinary.OmeV1beta1().InferenceServices("team-a").Get(context.Background(), "service", metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, int32(1), sentinel.calls.Load(), "ordinary injected warning behavior remains unchanged")
		})
	}
}

func TestActionRuntimeConstructorOwnsExecProviderBeforeConfigCopy(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(fmt.Sprint(failure), func(t *testing.T) {
			private := &runtime.Unknown{Raw: []byte(`{"PRIVATE_EXEC_CONFIG":"original"}`)}
			provider := &clientcmdapi.ExecConfig{Command: "/not-executed-in-constructor", APIVersion: "client.authentication.k8s.io/v1beta1", InteractiveMode: clientcmdapi.NeverExecInteractiveMode, Config: private}
			config := &rest.Config{Host: "https://example.invalid", ExecProvider: provider}
			if failure {
				config.TLSClientConfig.CAData = []byte("invalid CA")
			}
			before, err := json.Marshal(provider)
			require.NoError(t, err)
			_, err = (&defaultFactory{rest: config}).RuntimeClientForAction(context.Background())
			if failure {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Same(t, provider, config.ExecProvider)
			require.Same(t, private, config.ExecProvider.Config, "rest.CopyConfig must not replace caller's nested provider Config")
			after, err := json.Marshal(provider)
			require.NoError(t, err)
			require.JSONEq(t, string(before), string(after))
		})
	}
}

func TestActionClientsRefuseAllStandardRedirectsOnGETLISTAndDiscovery(t *testing.T) {
	for _, code := range []int{301, 302, 303, 307, 308} {
		for _, lane := range []string{"ome-get", "revision-list", "runtime-discovery"} {
			t.Run(fmt.Sprintf("%d/%s", code, lane), func(t *testing.T) {
				var selected, destination atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/unselected" {
						destination.Add(1)
						w.WriteHeader(403)
						return
					}
					selected.Add(1)
					w.Header().Set("Location", "/unselected")
					w.WriteHeader(code)
				}))
				defer server.Close()
				f := &defaultFactory{rest: &rest.Config{Host: server.URL}}
				var err error
				switch lane {
				case "ome-get":
					client, e := f.OMEClientForAction(context.Background())
					require.NoError(t, e)
					_, err = client.OmeV1beta1().InferenceServices("team-a").Get(context.Background(), "service", metav1.GetOptions{})
				case "revision-list":
					client, e := f.KubeClientForAction(context.Background())
					require.NoError(t, e)
					_, err = client.AppsV1().ControllerRevisions("team-a").List(context.Background(), metav1.ListOptions{Limit: 16})
				case "runtime-discovery":
					client, e := f.RuntimeClientForAction(context.Background())
					require.NoError(t, e)
					err = client.Get(context.Background(), types.NamespacedName{Namespace: "team-a", Name: "runtime"}, &v1beta1.ServingRuntime{})
				}
				require.Error(t, err)
				require.Zero(t, destination.Load(), "redirect must not reach an unselected path")
				if lane == "runtime-discovery" {
					require.LessOrEqual(t, selected.Load(), int32(2))
				} else {
					require.Equal(t, int32(1), selected.Load(), "no replay on selected GET/LIST")
				}
				require.Nil(t, f.ome)
				require.Nil(t, f.kube)
				require.Nil(t, f.runtime)
			})
		}
	}
}

func TestActionReadConfigPreservesOwnershipTimeoutWrapperAndCancellation(t *testing.T) {
	for _, timeout := range []time.Duration{0, 3 * time.Second, 30 * time.Second} {
		private := &runtime.Unknown{Raw: []byte(`{"PRIVATE":"value"}`)}
		provider := &clientcmdapi.ExecConfig{Config: private}
		config := &rest.Config{Host: "https://example.invalid", Timeout: timeout, ExecProvider: provider}
		// No transport construction with an invalid plugin; inspect ownership via
		// an intentionally failing construction, then test the actual wrapper.
		_, _, err := (&defaultFactory{rest: config}).actionConfigAndClient(context.Background())
		require.Error(t, err)
		require.Same(t, provider, config.ExecProvider)
		require.Same(t, private, provider.Config)
		require.Equal(t, timeout, config.Timeout)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	config := &rest.Config{Host: "http://example.invalid", Timeout: 20 * time.Millisecond, WrapTransport: func(inner http.RoundTripper) http.RoundTripper {
		return actionRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			<-r.Context().Done()
			return nil, r.Context().Err()
		})
	}}
	f := &defaultFactory{rest: config}
	owned, client, err := f.actionConfigAndClient(ctx)
	require.NoError(t, err)
	require.Equal(t, 20*time.Millisecond, owned.Timeout)
	require.Equal(t, 20*time.Millisecond, client.Timeout)
	require.IsType(t, rest.NoWarnings{}, owned.WarningHandler)
	require.IsType(t, rest.NoWarnings{}, owned.WarningHandlerWithContext)
	request, err := http.NewRequest("GET", "http://example.invalid", nil)
	require.NoError(t, err)
	_, err = client.Do(request)
	require.Error(t, err)
	require.Equal(t, int32(1), calls.Load())
	require.NoError(t, ctx.Err(), "narrow request timeout must not cancel the action")
	cancel()
	_, err = f.OMEClientForAction(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, err = f.KubeClientForAction(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, _, err = f.actionConfigAndClient(nil) //nolint:staticcheck // Deliberately test the nil-context refusal boundary.
	require.Error(t, err)
	for _, timeout := range []time.Duration{0, 30 * time.Second} {
		owned, client, err = (&defaultFactory{rest: &rest.Config{Host: "http://example.invalid", Timeout: timeout}}).actionConfigAndClient(context.Background())
		require.NoError(t, err)
		require.Equal(t, 10*time.Second, owned.Timeout)
		require.Equal(t, 10*time.Second, client.Timeout)
	}
}
