package transport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	"k8s.io/client-go/rest"
)

type guardedScaleTransport interface {
	PatchInferenceReplicaScale(context.Context, Resource, []byte, JSONPatchOptions) (*autoscalingv1.Scale, error)
}

func TestGuardedScaleUsesExactOneShotSubresourcePatch(t *testing.T) {
	for _, dry := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "server dry run"}[dry], func(t *testing.T) {
			calls := 0
			patch := `[{"op":"test","path":"/metadata/uid","value":"ir-uid"},{"op":"test","path":"/metadata/resourceVersion","value":"81"},{"op":"replace","path":"/spec/replicas","value":3}]`
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				require.Equal(t, "PATCH", r.Method)
				require.Equal(t, "/apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine/scale", r.URL.Path)
				require.Equal(t, "application/json-patch+json", r.Header.Get("Content-Type"))
				require.Equal(t, map[bool]string{false: "", true: "All"}[dry], r.URL.Query().Get("dryRun"))
				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Equal(t, patch, string(body))
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{"name":"chat-engine","namespace":"prod","uid":"ir-uid","resourceVersion":"82"},"spec":{"replicas":3},"status":{"replicas":1,"selector":"private-selector"}}`)
			}))
			t.Cleanup(server.Close)
			client, err := NewBounded(&rest.Config{Host: server.URL}, 1<<20)
			require.NoError(t, err)
			scaler, ok := any(client).(guardedScaleTransport)
			require.True(t, ok, "transport must provide guarded PATCH /scale")
			got, err := scaler.PatchInferenceReplicaScale(context.Background(), Resource{Namespace: "prod", Resource: "inferencereplicas", Name: "chat-engine"}, []byte(patch), JSONPatchOptions{DryRun: dry})
			require.NoError(t, err)
			require.Equal(t, int32(3), got.Spec.Replicas)
			require.Equal(t, 1, calls)
		})
	}
}
