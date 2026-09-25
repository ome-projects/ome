package transport

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestGetInferenceServiceIsSingleUncachedNoRetryRequest(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/apis/ome.io/v1beta1/namespaces/prod/inferenceservices/chat", r.URL.Path)
		assert.Empty(t, r.URL.Query().Get("watch"))
		w.Header().Set("Content-Type", "application/json")
		if !assert.NoError(t, json.NewEncoder(w).Encode(&omev1beta1.InferenceService{
			TypeMeta:   metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"},
			ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: "uid-chat", ResourceVersion: "42"},
		})) {
			return
		}
	}))
	defer server.Close()
	client, err := NewBounded(&rest.Config{Host: server.URL}, 1024)
	require.NoError(t, err)
	service, err := client.GetInferenceService(context.Background(), "prod", "chat", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "chat", service.Name)
	require.EqualValues(t, 1, calls.Load())
}

func TestGetInferenceServiceRejectsAmbiguousOrOversizedResponses(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want error
	}{
		{"duplicate identity", `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"chat","name":"other","namespace":"prod","uid":"uid-chat","resourceVersion":"42"}}`, ErrResponseIdentity},
		{"wrong casing", `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"Name":"chat","namespace":"prod","uid":"uid-chat","resourceVersion":"42"}}`, ErrResponseIdentity},
		{"missing api version", `{"kind":"InferenceService","metadata":{"name":"chat","namespace":"prod","uid":"uid-chat","resourceVersion":"42"}}`, ErrResponseIdentity},
		{"missing kind", `{"apiVersion":"ome.io/v1beta1","metadata":{"name":"chat","namespace":"prod","uid":"uid-chat","resourceVersion":"42"}}`, ErrResponseIdentity},
		{"wrong api version", `{"apiVersion":"other.io/v1","kind":"InferenceService","metadata":{"name":"chat","namespace":"prod","uid":"uid-chat","resourceVersion":"42"}}`, ErrResponseIdentity},
		{"wrong kind", `{"apiVersion":"ome.io/v1beta1","kind":"Other","metadata":{"name":"chat","namespace":"prod","uid":"uid-chat","resourceVersion":"42"}}`, ErrResponseIdentity},
		{"wrong name", `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"other","namespace":"prod","uid":"uid-chat","resourceVersion":"42"}}`, ErrResponseIdentity},
		{"wrong namespace", `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"chat","namespace":"other","uid":"uid-chat","resourceVersion":"42"}}`, ErrResponseIdentity},
		{"root status alias", `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"chat","namespace":"prod","uid":"uid-chat","resourceVersion":"42"},"Status":{"rollout":{}}}`, ErrResponseIdentity},
		{"nested rollout alias", `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"chat","namespace":"prod","uid":"uid-chat","resourceVersion":"42"},"status":{"rollout":{},"Rollout":{}}}`, ErrResponseIdentity},
		{"nested active run alias", `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"chat","namespace":"prod","uid":"uid-chat","resourceVersion":"42"},"status":{"rollout":{"activeRun":{},"ActiveRun":{}}}}`, ErrResponseIdentity},
		{"spec group alias", `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"chat","namespace":"prod","uid":"uid-chat","resourceVersion":"42"},"spec":{"rollout":{"groups":[{"components":["engine"],"Canary":{}}]}}}`, ErrResponseIdentity},
		{"duplicate nested evidence", `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"chat","namespace":"prod","uid":"uid-chat","resourceVersion":"42","annotations":{"ome.io/rollout-repin":"rp1:aaaaaaaaaaaa","ome.io/rollout-repin":"rp1:bbbbbbbbbbbb"}}}`, ErrResponseIdentity},
		{"oversized", `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"chat","namespace":"prod","uid":"uid-chat","resourceVersion":"42"},"padding":"` + strings.Repeat("x", 2048) + `"}`, ErrResponseTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			client, err := NewBounded(&rest.Config{Host: server.URL}, 1024)
			require.NoError(t, err)
			_, err = client.GetInferenceService(context.Background(), "prod", "chat", metav1.GetOptions{})
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestGetInferenceServiceAllowsUnknownFutureFields(t *testing.T) {
	body := `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"chat","namespace":"prod","uid":"uid-chat","resourceVersion":"42","futureMetadata":{"Status":{}}},"spec":{"futureSpec":{"Rollout":{}}},"status":{"futureStatus":{"ActiveRun":{}}},"FutureRoot":{"Spec":{}}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	defer server.Close()
	client, err := NewBounded(&rest.Config{Host: server.URL}, 4096)
	require.NoError(t, err)
	service, err := client.GetInferenceService(context.Background(), "prod", "chat", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "chat", service.Name)
}

func TestGetInferenceServiceRefusesRedirectWithoutFollowing(t *testing.T) {
	var destination atomic.Int32
	dst := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { destination.Add(1) }))
	defer dst.Close()
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", dst.URL+"/private")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer src.Close()
	client, err := NewBounded(&rest.Config{Host: src.URL}, 1024)
	require.NoError(t, err)
	_, err = client.GetInferenceService(context.Background(), "prod", "chat", metav1.GetOptions{})
	require.Error(t, err)
	require.Zero(t, destination.Load())
}
