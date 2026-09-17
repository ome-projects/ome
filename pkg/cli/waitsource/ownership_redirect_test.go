package waitsource

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestWaitSourceConstructorPreservesExecConfigOwnership(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(fmt.Sprint(bad), func(t *testing.T) {
			object := &runtime.Unknown{Raw: []byte(`{"private":"constructor-only"}`)}
			provider := &clientcmdapi.ExecConfig{Command: "never-run-constructor-plugin", APIVersion: "client.authentication.k8s.io/v1", InteractiveMode: clientcmdapi.NeverExecInteractiveMode, Config: object}
			config := &rest.Config{Host: "http://example.invalid", Timeout: 20 * time.Millisecond, ExecProvider: provider}
			if bad {
				config.Host = "://malformed"
			}
			_, err := NewInferenceService(config, "prod", "chat")
			if bad {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Same(t, provider, config.ExecProvider)
			require.Same(t, object, provider.Config)
			require.Equal(t, 20*time.Millisecond, config.Timeout)
			require.Equal(t, []byte(`{"private":"constructor-only"}`), object.Raw)
		})
	}
}

func TestWaitSourceRefusesAllRedirectDestinations(t *testing.T) {
	for _, status := range []int{301, 302, 303, 307, 308} {
		for _, watching := range []bool{false, true} {
			for _, otherHost := range []bool{false, true} {
				t.Run(fmt.Sprintf("%d/watch=%t/other=%t", status, watching, otherHost), func(t *testing.T) {
					var destinationReads atomic.Int32
					destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						destinationReads.Add(1)
						_, _ = io.WriteString(w, `{"private":"redirect-destination"}`)
					}))
					defer destination.Close()
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.URL.Path == "/api/v1/namespaces/prod/secrets/unselected" {
							destinationReads.Add(1)
							return
						}
						location := "/api/v1/namespaces/prod/secrets/unselected"
						if otherHost {
							location = destination.URL + location
						}
						w.Header().Set("Location", location)
						w.WriteHeader(status)
					}))
					defer server.Close()
					source, err := NewInferenceService(&rest.Config{Host: server.URL}, "prod", "chat")
					require.NoError(t, err)
					if watching {
						w, e := source.Watch(context.Background(), "opaque-rv")
						if w != nil {
							w.Stop()
						}
						require.Error(t, e)
					} else {
						_, e := source.Get(context.Background())
						require.Error(t, e)
					}
					require.Zero(t, destinationReads.Load(), "named observation cannot follow a redirect into an unselected path/host")
				})
			}
		}
	}
}

type waitWarnings struct{ calls atomic.Int32 }

func (w *waitWarnings) HandleWarningHeader(int, string, string) { w.calls.Add(1) }
func (w *waitWarnings) HandleWarningHeaderWithContext(context.Context, int, string, string) {
	w.calls.Add(1)
}

func TestWaitSourcePreservesAuthWrappersAndWarningOwnership(t *testing.T) {
	warnings := &waitWarnings{}
	var wrapped atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer constructor-selected-token" {
			t.Error("selected authentication missing")
		}
		w.Header().Set("Warning", `299 selected "PRIVATE-WARNING"`)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"chat","namespace":"prod","uid":"uid","resourceVersion":"rv"}}`)
	}))
	defer server.Close()
	config := &rest.Config{Host: server.URL, BearerToken: "constructor-selected-token", Timeout: 40 * time.Millisecond, WarningHandler: warnings, WarningHandlerWithContext: warnings,
		WrapTransport: func(base http.RoundTripper) http.RoundTripper { wrapped.Add(1); return base }}
	source, err := NewInferenceService(config, "prod", "chat")
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if wrapped.Load() != 1 || warnings.calls.Load() != 0 {
		t.Fatal("wrapper/warning boundary changed")
	}
	if config.WarningHandler != warnings || config.WarningHandlerWithContext != warnings || config.Timeout != 40*time.Millisecond || source.config.Timeout != 40*time.Millisecond {
		t.Fatal("caller configuration ownership changed")
	}
}
