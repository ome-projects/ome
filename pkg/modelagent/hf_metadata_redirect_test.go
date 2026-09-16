package modelagent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHfMetadataFollowsSameOriginAlias(t *testing.T) {
	for _, manifest := range []bool{false, true} {
		t.Run(map[bool]string{false: "revision", true: "manifest"}[manifest], func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				require.Equal(t, "Bearer token", r.Header.Get("Authorization"))
				if r.URL.Path == "/api/models/alias/revision/main" || r.URL.Path == "/api/models/alias/revision/"+testDirectHfSHA {
					http.Redirect(w, r, "/api/models/org/model/revision/"+testDirectHfSHA+"?"+r.URL.RawQuery, http.StatusTemporaryRedirect)
					return
				}
				require.Equal(t, "/api/models/org/model/revision/"+testDirectHfSHA, r.URL.Path)
				if manifest {
					require.Equal(t, "true", r.URL.Query().Get("blobs"))
				}
				_ = json.NewEncoder(w).Encode(testHfSnapshotManifest(map[string]string{"config.json": "{}"}))
			}))
			defer server.Close()
			if manifest {
				got, err := fetchHfSnapshotManifest(context.Background(), "alias", testDirectHfSHA, "token", server.URL)
				require.NoError(t, err)
				require.Equal(t, testDirectHfSHA, got.SHA)
			} else {
				sha, err := resolveHfRevision(context.Background(), "alias", "main", "token", server.URL)
				require.NoError(t, err)
				require.Equal(t, testDirectHfSHA, sha)
			}
			require.EqualValues(t, 2, requests.Load())
		})
	}
}

func TestHfMetadataBoundsRedirects(t *testing.T) {
	for _, manifest := range []bool{false, true} {
		t.Run(map[bool]string{false: "revision", true: "manifest"}[manifest], func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				http.Redirect(w, r, r.URL.RequestURI(), http.StatusTemporaryRedirect)
			}))
			defer server.Close()
			var err error
			if manifest {
				_, err = fetchHfSnapshotManifest(context.Background(), "alias", testDirectHfSHA, "token", server.URL)
			} else {
				_, err = resolveHfRevision(context.Background(), "alias", "main", "token", server.URL)
			}
			require.ErrorContains(t, err, "307")
			require.EqualValues(t, 4, requests.Load())
		})
	}
}

func TestHfMetadataRejectsRedirectOriginChanges(t *testing.T) {
	initial, err := http.NewRequest(http.MethodGet, "https://hub.example/api/models/alias", nil)
	require.NoError(t, err)
	for _, target := range []string{
		"http://hub.example/api/models/model",
		"https://other.example/api/models/model",
		"https://hub.example:8443/api/models/model",
		"https://user@hub.example/api/models/model",
	} {
		t.Run(target, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodGet, target, nil)
			require.NoError(t, err)
			require.ErrorIs(t, checkHfMetadataRedirect(request, []*http.Request{initial}), http.ErrUseLastResponse)
		})
	}
}
