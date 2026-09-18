package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

func TestBoundedDynamicExactGETDoesNotFollowRedirect(t *testing.T) {
	followed := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		followed = true
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "GET", r.Method)
		require.Equal(t, "/apis/keda.sh/v1alpha1/namespaces/prod/scaledobjects/scaledobject-chat-engine", r.URL.Path)
		http.Redirect(w, r, target.URL+"/private", http.StatusFound)
	}))
	defer source.Close()
	client, err := NewBoundedDynamic(&rest.Config{Host: source.URL}, 1<<20)
	require.NoError(t, err)
	_, err = client.Resource(schema.GroupVersionResource{Group: "keda.sh", Version: "v1alpha1", Resource: "scaledobjects"}).
		Namespace("prod").Get(context.Background(), "scaledobject-chat-engine", metav1.GetOptions{})
	require.Error(t, err)
	require.False(t, followed)
}
