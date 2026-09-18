package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

func TestBoundedInferenceReplicaGETRejectsOversize(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "GET", r.Method)
		require.Equal(t, "/apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/chat-engine", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(strings.Repeat("secret-token", 1000)))
	}))
	defer server.Close()
	client, err := NewBounded(&rest.Config{Host: server.URL}, 256)
	require.NoError(t, err)
	_, err = client.GetInferenceReplica(context.Background(), "prod", "chat-engine", metav1.GetOptions{})
	require.ErrorIs(t, err, ErrResponseTooLarge)
}
