package transport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

func TestJSONPatchRejectsAmbiguousResponseIdentityWithoutReplay(t *testing.T) {
	for _, body := range []string{
		`{"kind":"Secret","Kind":"InferenceService"}`,
		`{"kind":"Secret","\u212aind":"InferenceService"}`,
		`{"apiVersion":"other/v1","APIVersion":"ome.io/v1beta1"}`,
		`{"metadata":{"name":"other"},"Metadata":{"name":"demo"}}`,
		`{"metadata":{"name":"other","Name":"demo"}}`,
		`{"metadata":{"namespace":"other","Namespace":"team-a"}}`,
		`{"metadata":{"uid":"other","UID":"uid-demo"}}`,
		`{"metadata":{"resourceVersion":"other","ResourceVersion":"42"}}`,
		`{"kind":"Secret","kind":"InferenceService"}`,
		`{"metadata":{"uid":"other","uid":"uid-demo"}}`,
		`{"metadata":null}`,
		`{"metadata":[]}`,
		`{} {}`,
		`[]`,
		`not JSON`,
	} {
		t.Run(body, func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, body)
			}))
			t.Cleanup(server.Close)
			client, err := NewBounded(&rest.Config{Host: server.URL}, 64*1024)
			require.NoError(t, err)
			got, err := client.JSONPatch(context.Background(), Resource{Namespace: "team-a", Resource: "inferenceservices", Name: "demo"}, []byte(`[]`), JSONPatchOptions{})
			require.ErrorIs(t, err, ErrResponseIdentity, "ambiguous identity cannot prove acceptance")
			require.Nil(t, got)
			require.Equal(t, int32(1), attempts.Load(), "uncertain response must not replay")
		})
	}
}

func TestJSONPatchPreservesUnambiguousRawResponseAndUnknownFields(t *testing.T) {
	body := " {\"apiVersion\":\"ome.io/v1beta1\",\"kind\":\"InferenceService\",\"metadata\":{\"name\":\"demo\",\"namespace\":\"team-a\",\"uid\":\"uid-demo\",\"resourceVersion\":\"42\",\"futureMetadata\":{}},\"futureStatus\":{\"name\":\"other\"}}\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	client, err := NewBounded(&rest.Config{Host: server.URL}, 64*1024)
	require.NoError(t, err)
	got, err := client.JSONPatch(context.Background(), Resource{Namespace: "team-a", Resource: "inferenceservices", Name: "demo"}, []byte(`[]`), JSONPatchOptions{})
	require.NoError(t, err)
	require.Equal(t, body, string(got), "identity checks must not rewrite raw bytes or reject future fields")
}
