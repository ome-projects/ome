package transport

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// A redirect may change method, replay a write, or drop dryRun=All even at
// the identical path. Every destination is a credential-free local fixture.
func TestJSONPatchRedirectsNeverReplayOrDropServerDryRun(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		for _, dryRun := range []bool{false, true} {
			for _, status := range []int{301, 302, 303, 307, 308} {
				for _, destination := range []string{"same path without query", "unselected secret path", "other local host"} {
					t.Run(fmt.Sprintf("bounded=%t/dryRun=%t/%d/%s", bounded, dryRun, status, destination), func(t *testing.T) {
						var requests, followed atomic.Int32
						other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							followed.Add(1)
							w.Header().Set("Content-Type", "application/json")
							_, _ = io.WriteString(w, `{}`)
						}))
						t.Cleanup(other.Close)
						path := "/apis/ome.io/v1beta1/namespaces/team-a/inferenceservices/demo"
						patch := `[{"op":"test","path":"/metadata/resourceVersion","value":"42"},{"op":"add","path":"/metadata/annotations/ome.io~1rollout-rollback","value":"true"}]`
						server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							w.Header().Set("Content-Type", "application/json")
							if requests.Add(1) != 1 {
								followed.Add(1)
								_, _ = io.WriteString(w, `{}`)
								return
							}
							body, err := io.ReadAll(r.Body)
							if err != nil || r.Method != "PATCH" || r.URL.Path != path || string(body) != patch || r.Header.Get("Content-Type") != "application/json-patch+json" {
								t.Error("original mutation wire contract changed")
							}
							want := ""
							if dryRun {
								want = "All"
							}
							if r.URL.Query().Get("dryRun") != want {
								t.Error("original dry-run query changed")
							}
							location := path
							if destination == "unselected secret path" {
								location = "/api/v1/namespaces/team-a/secrets/private-fixture"
							}
							if destination == "other local host" {
								location = other.URL + path
							}
							w.Header().Set("Location", location)
							w.WriteHeader(status)
							_, _ = io.WriteString(w, `{}`)
						}))
						t.Cleanup(server.Close)
						config := &rest.Config{Host: server.URL}
						var client *Client
						var err error
						if bounded {
							client, err = NewBounded(config, 64*1024)
						} else {
							client, err = New(config)
						}
						require.NoError(t, err)
						_, err = client.JSONPatch(context.Background(), Resource{Namespace: "team-a", Resource: "inferenceservices", Name: "demo"}, []byte(patch), JSONPatchOptions{DryRun: dryRun})
						require.Error(t, err, "redirect is not API acceptance")
						require.Equal(t, int32(1), requests.Load(), "mutation must never replay, even to the same path")
						require.Zero(t, followed.Load(), "no method-changed read or query-stripped write")
					})
				}
			}
		}
	}
}

func TestNewClientsPreserveCallerOwnedExecConfiguration(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		t.Run(fmt.Sprint(bounded), func(t *testing.T) {
			original := &runtime.Unknown{Raw: []byte(`{"private":"PRIVATE_SENTINEL"}`)}
			provider := &clientcmdapi.ExecConfig{Command: "never-executed", APIVersion: "client.authentication.k8s.io/v1beta1", InteractiveMode: clientcmdapi.NeverExecInteractiveMode, Config: original}
			config := &rest.Config{Host: "https://never-contact.invalid", ExecProvider: provider}
			var err error
			if bounded {
				_, err = NewBounded(config, 64*1024)
			} else {
				_, err = New(config)
			}
			require.NoError(t, err)
			require.Same(t, provider, config.ExecProvider)
			require.Same(t, original, provider.Config, "constructor cannot replace a caller-owned auth field")
			require.Equal(t, `{"private":"PRIVATE_SENTINEL"}`, string(original.Raw))
		})
	}
}
