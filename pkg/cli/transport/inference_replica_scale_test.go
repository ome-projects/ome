package transport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

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

func TestScaleDecoderRequiresExplicitObservedCounts(t *testing.T) {
	for _, body := range []string{
		`{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{"name":"x","namespace":"prod","uid":"uid-x","resourceVersion":"1"},"spec":{"replicas":1}}`,
		`{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{"name":"x","namespace":"prod","uid":"uid-x","resourceVersion":"1"},"spec":{"replicas":1},"status":{}}`,
		`{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{"name":"x","namespace":"prod","uid":"uid-x","resourceVersion":"1"},"spec":{"replicas":1},"status":{"replicas":null}}`,
	} {
		_, err := decodeScaleResponse([]byte(body))
		require.ErrorIs(t, err, ErrResponseIdentity, "omission/null is not an observed zero")
	}
}

func TestScaleDecoderCompleteAndMalformedMatrix(t *testing.T) {
	valid := `{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{"name":"x","namespace":"prod","uid":"uid-x","resourceVersion":"1"},"spec":{"replicas":1},"status":{"replicas":0,"selector":"private"}}`
	got, err := decodeScaleResponse([]byte(valid))
	require.NoError(t, err)
	require.Zero(t, got.Status.Replicas, "explicit observed zero remains zero")
	for _, raw := range []string{`null`, `{`, `[]`, `{} {}`, `{"apiVersion":"autoscaling/v1","kind":"Other","metadata":{},"spec":{"replicas":1},"status":{"replicas":0}}`, `{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{},"spec":{"replicas":1,"replicas":1},"status":{"replicas":0}}`, `{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{},"spec":{"Replicas":1},"status":{"replicas":0}}`, `{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{},"spec":{"replicas":1},"status":{"replicas":0,"Selector":"private"}}`, `{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{},"spec":{"replicas":1},"status":{"replicas":0,"replicas":0}}`, `{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{},"spec":{"replicas":null},"status":{"replicas":0}}`, `{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{},"spec":{"replicas":1},"status":{"replicas":2147483648}}`, `{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{},"spec":{"replicas":1},"status":{"replicas":-1}}`} {
		_, err := decodeScaleResponse([]byte(raw))
		require.ErrorIs(t, err, ErrResponseIdentity, raw)
	}
	for _, raw := range []string{`{`, `{}`, `{"replicas":null}`, `{"replicas":-1}`, `{"replicas":1.5}`, `{"replicas":2147483648}`} {
		require.False(t, explicitScaleCount([]byte(raw)))
	}
	require.True(t, explicitScaleCount([]byte(`{"replicas":0}`)))
}

type scaleWarningCounter struct{ calls atomic.Int32 }

func (s *scaleWarningCounter) HandleWarningHeader(int, string, string) { s.calls.Add(1) }
func (s *scaleWarningCounter) HandleWarningHeaderWithContext(context.Context, int, string, string) {
	s.calls.Add(1)
}

func TestScaleTransportTimeoutCancellationAndWarningPrivacy(t *testing.T) {
	for _, mode := range []string{"timeout", "cancel", "warnings"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			started := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				close(started)
				if mode != "warnings" {
					select {
					case <-r.Context().Done():
						return
					case <-time.After(time.Second):
						return
					}
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Warning", `299 synthetic "private-warning"`)
				_, _ = io.WriteString(w, `{"apiVersion":"autoscaling/v1","kind":"Scale","metadata":{"name":"x","namespace":"prod","uid":"uid-x","resourceVersion":"1"},"spec":{"replicas":1},"status":{"replicas":0}}`)
			}))
			defer server.Close()
			warnings := &scaleWarningCounter{}
			config := &rest.Config{Host: server.URL, Timeout: 40 * time.Millisecond, WarningHandler: warnings, WarningHandlerWithContext: warnings}
			client, err := NewBounded(config, 1<<20)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancel" {
				go func() { <-started; cancel() }()
			}
			_, err = client.PatchInferenceReplicaScale(ctx, Resource{Namespace: "prod", Resource: "inferencereplicas", Name: "x"}, []byte(`[]`), JSONPatchOptions{})
			if mode == "warnings" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, int32(1), calls.Load(), "no retry after timeout or cancellation")
			require.Zero(t, warnings.calls.Load(), "neither warning handler may expose headers")
			require.Equal(t, 40*time.Millisecond, config.Timeout)
		})
	}
}
