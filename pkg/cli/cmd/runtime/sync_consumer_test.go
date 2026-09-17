package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

type ordinarySyncWarnings struct{ calls *int }

func (h ordinarySyncWarnings) HandleWarningHeader(int, string, string) { *h.calls++ }
func (h ordinarySyncWarnings) HandleWarningHeaderWithContext(context.Context, int, string, string) {
	*h.calls++
}

func TestRuntimeSyncOrdinaryRedirectConsumerAllFormats(t *testing.T) {
	for _, code := range []int{301, 302, 303, 307, 308} {
		for _, format := range []string{"table", "wide", "json", "yaml"} {
			for _, mode := range []string{"server", "none"} {
				t.Run(fmt.Sprintf("%d/%s/%s", code, format, mode), func(t *testing.T) {
					f, _ := syncFixture(t)
					attempts, dest, warnings := 0, 0, 0
					target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { dest++ }))
					defer target.Close()
					origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						attempts++
						require.Equal(t, "PATCH", r.Method)
						require.Equal(t, mode == "server", r.URL.Query().Get("dryRun") == "All")
						w.Header().Set("Location", target.URL+"/api/v1/namespaces/private/secrets/PRIVATE")
						w.Header().Set("Warning", `299 fixture "PRIVATE_WARNING"`)
						w.Header().Set("Retry-After", "0")
						w.WriteHeader(code)
					}))
					defer origin.Close()
					f.config.Host = origin.URL
					f.config.WarningHandler = ordinarySyncWarnings{&warnings}
					f.config.WarningHandlerWithContext = ordinarySyncWarnings{&warnings}
					var out, errOut bytes.Buffer
					c := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
					c.SilenceErrors, c.SilenceUsage = true, true
					c.SetArgs([]string{"sync", "service", "--yes", "--dry-run=" + mode, "-o=" + format})
					err := c.Execute()
					require.Error(t, err)
					require.Equal(t, 1, attempts)
					require.Zero(t, dest)
					require.Zero(t, warnings)
					require.Empty(t, out.String())
					require.NotContains(t, err.Error()+errOut.String(), "PRIVATE")
				})
			}
		}
	}
}

func TestRuntimeSyncOrdinaryWarningAndCancellationConsumer(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		for _, scenario := range []string{"success", "server dry", "wrapped canceled", "wrapped deadline", "empty RV", "alias Unicode", "trailing", "scalar", "stdout writer"} {
			t.Run(format+"/"+scenario, func(t *testing.T) {
				f, v := syncFixture(t)
				attempts, warnings := 0, 0
				f.config.WarningHandler = ordinarySyncWarnings{&warnings}
				f.config.WarningHandlerWithContext = ordinarySyncWarnings{&warnings}
				host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					attempts++
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Warning", `299 fixture "PRIVATE_WARNING"`)
					switch scenario {
					case "success", "server dry", "stdout writer":
						before, err := json.Marshal(v)
						require.NoError(t, err)
						body, err := io.ReadAll(r.Body)
						require.NoError(t, err)
						patch, err := jsonpatch.DecodePatch(body)
						require.NoError(t, err)
						after, err := patch.Apply(before)
						require.NoError(t, err)
						_, _ = w.Write(after)
						return
					case "empty RV":
						v.ResourceVersion = ""
					case "alias Unicode":
						_, _ = io.WriteString(w, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"service","namespace":"team-a","uid":"service-uid","resourceVersion":"15","ResourceVerſion":"PRIVATE_ALIAS"}}`)
						return
					case "trailing":
						_ = json.NewEncoder(w).Encode(v)
						_, _ = io.WriteString(w, `{"PRIVATE":true}`)
						return
					case "scalar":
						_, _ = io.WriteString(w, `"PRIVATE_BODY"`)
						return
					}
					_ = json.NewEncoder(w).Encode(v)
				}))
				defer host.Close()
				f.config.Host = host.URL
				var expected error
				switch scenario {
				case "wrapped canceled":
					expected = context.Canceled
				case "wrapped deadline":
					expected = context.DeadlineExceeded
				}
				if expected != nil {
					f.config.WrapTransport = func(http.RoundTripper) http.RoundTripper {
						return ordinarySyncRoundTripper(func(*http.Request) (*http.Response, error) {
							attempts++
							return nil, fmt.Errorf("PRIVATE_CREDENTIAL: %w", expected)
						})
					}
				}
				var out, errOut bytes.Buffer
				streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut}
				if scenario == "stdout writer" {
					streams.Out = syncFailWriter{}
				}
				c := NewCmd(f, streams)
				c.SilenceErrors, c.SilenceUsage = true, true
				mode := "none"
				if scenario == "server dry" {
					mode = "server"
				}
				c.SetArgs([]string{"sync", "service", "--yes", "--dry-run=" + mode, "-o=" + format})
				err := c.Execute()
				require.Equal(t, 1, attempts)
				require.Zero(t, warnings)
				require.NotContains(t, errOut.String()+out.String(), "PRIVATE")
				if scenario == "success" || scenario == "server dry" {
					require.NoError(t, err)
					require.NotEmpty(t, out.String())
					return
				}
				require.Error(t, err)
				require.NotContains(t, err.Error(), "PRIVATE")
				require.Empty(t, out.String())
				if expected != nil {
					require.ErrorIs(t, err, expected)
					require.Contains(t, err.Error(), "outcome unknown")
				}
			})
		}
	}
}

type ordinarySyncRoundTripper func(*http.Request) (*http.Response, error)

func (f ordinarySyncRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRuntimeSyncOrdinaryConfigOwnershipAndShortTimeout(t *testing.T) {
	for _, mode := range []string{"client", "server", "none"} {
		t.Run(mode, func(t *testing.T) {
			f, _ := syncFixture(t)
			object := &runtime.Unknown{Raw: []byte(`{"private":"EXEC_CONFIG"}`)}
			provider := &clientcmdapi.ExecConfig{APIVersion: "PRIVATE_INVALID", Command: "never-run-private-plugin", InteractiveMode: clientcmdapi.NeverExecInteractiveMode, Config: object}
			f.config.ExecProvider = provider
			f.config.Timeout = time.Millisecond * 17
			before := rest.CopyConfig(&rest.Config{Host: f.config.Host, Timeout: f.config.Timeout})
			var out, errOut bytes.Buffer
			c := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
			c.SilenceErrors, c.SilenceUsage = true, true
			c.SetArgs([]string{"sync", "service", "--yes", "--dry-run=" + mode})
			err := c.Execute()
			require.Error(t, err)
			require.NotContains(t, err.Error()+errOut.String(), "PRIVATE")
			require.Same(t, provider, f.config.ExecProvider)
			require.Same(t, object, provider.Config)
			require.Equal(t, before.Timeout, f.config.Timeout)
			require.Equal(t, []byte(`{"private":"EXEC_CONFIG"}`), object.Raw)
			require.Empty(t, out.String())
		})
	}
}

func TestRuntimeSyncOrdinaryParserClosedExtraFlags(t *testing.T) {
	for _, flag := range []string{"--yes=PRIVATE", "--discard-pending-actions=PRIVATE", "--override-analysis=PRIVATE", "--wait=PRIVATE", "--force=PRIVATE"} {
		f := &validationFactory{}
		var out, errOut bytes.Buffer
		c := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
		c.SilenceErrors, c.SilenceUsage = true, true
		c.SetArgs([]string{"sync", "service", flag})
		err := c.Execute()
		require.Error(t, err)
		require.Zero(t, f.namespaceCalls)
		require.NotContains(t, err.Error()+errOut.String()+out.String(), "PRIVATE")
	}
}

func TestRuntimeSyncOrdinarySuccessfulExecConfigOwnershipNoPlugin(t *testing.T) {
	f, _ := syncFixture(t)
	object := &runtime.Unknown{Raw: []byte(`{"private":"EXEC_CONFIG"}`)}
	provider := &clientcmdapi.ExecConfig{APIVersion: "client.authentication.k8s.io/v1beta1", Command: "never-run-private-plugin", InteractiveMode: clientcmdapi.NeverExecInteractiveMode, Config: object}
	f.config.ExecProvider = provider
	f.config.Timeout = 17 * time.Millisecond
	var out, errOut bytes.Buffer
	c := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
	c.SilenceErrors, c.SilenceUsage = true, true
	c.SetArgs([]string{"sync", "service", "--yes", "--dry-run=client", "-o=json"})
	require.NoError(t, c.Execute())
	require.Same(t, provider, f.config.ExecProvider)
	require.Same(t, object, provider.Config)
	require.Equal(t, []byte(`{"private":"EXEC_CONFIG"}`), object.Raw)
	require.Equal(t, 17*time.Millisecond, f.config.Timeout)
	require.NotContains(t, out.String()+errOut.String(), "PRIVATE")
}
