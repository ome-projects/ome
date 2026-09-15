package rollout

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestGuardedRolloutClientDryRunPreservesExecOwnership(t *testing.T) {
	for _, action := range []string{"promote", "rollback"} {
		t.Run(action, func(t *testing.T) {
			v, rt, ir := canaryActionFixture(t, false)
			patches := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if q.Method == "PATCH" {
					patches++
					t.Error("client dry-run sent patch")
					return
				}
				serveCanaryRead(t, w, q, v, ir)
			}))
			defer server.Close()
			f := newWireFactory(t, server, rt)
			original := &runtime.Unknown{Raw: []byte(`{"private":"PRIVATE_SENTINEL"}`)}
			provider := &clientcmdapi.ExecConfig{Command: "never-executed", APIVersion: "client.authentication.k8s.io/v1beta1", InteractiveMode: clientcmdapi.NeverExecInteractiveMode, Config: original}
			f.config.ExecProvider = provider
			var out, stderr bytes.Buffer
			cmd := newCmdWithClock(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, canaryClock)
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs([]string{action, "chat", "--yes", "--dry-run=client", "-o=json"})
			if err := cmd.Execute(); err != nil || patches != 0 || out.Len() == 0 {
				t.Fatalf("positive client dryrun err=%v patches=%d", err, patches)
			}
			if f.config.ExecProvider != provider || provider.Config != original || string(original.Raw) != `{"private":"PRIVATE_SENTINEL"}` {
				t.Fatal("actual rollout runner changed caller-owned exec configuration before transport construction")
			}
		})
	}
}

func TestGuardedRolloutMutationWarningsDoNotDiscloseRawServerText(t *testing.T) {
	for _, action := range []string{"promote", "rollback"} {
		for _, malformed := range []bool{false, true} {
			t.Run(action+map[bool]string{false: "/accepted", true: "/ambiguous"}[malformed], func(t *testing.T) {
				v, rt, ir := canaryActionFixture(t, false)
				patches := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if q.Method != "PATCH" {
						serveCanaryRead(t, w, q, v, ir)
						return
					}
					patches++
					w.Header().Set("Warning", `299 private.example "PRIVATE_TOKEN sk-proj-0123456789abcdefghijklmnopqrstuvwxyz https://private.example/provider"`)
					if malformed {
						w.Write([]byte(`{"metadata":null}`))
						return
					}
					json.NewEncoder(w).Encode(v)
				}))
				defer server.Close()
				f := newWireFactory(t, server, rt)
				var out, stderr, warnings bytes.Buffer
				f.config.WarningHandler = rest.NewWarningWriter(&warnings, rest.WarningWriterOptions{})
				cmd := newCmdWithClock(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, canaryClock)
				cmd.SilenceErrors, cmd.SilenceUsage = true, true
				cmd.SetArgs([]string{action, "chat", "--yes", "--dry-run=server", "-o=json"})
				err := cmd.Execute()
				if (err != nil) != malformed || patches != 1 {
					t.Fatalf("positive response err=%v patches=%d", err, patches)
				}
				if malformed && out.Len() != 0 {
					t.Fatal("ambiguous response produced result")
				}
				if warnings.Len() != 0 || strings.Contains(out.String()+stderr.String(), "PRIVATE_TOKEN") {
					t.Fatal("actual mutation dispatched private Warning header to inherited writer")
				}
			})
		}
	}
}
