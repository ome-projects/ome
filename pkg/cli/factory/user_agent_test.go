package factory

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omeversion "sigs.k8s.io/ome/pkg/version"
)

func TestRESTConfigAddsBoundedProductUserAgent(t *testing.T) {
	defaultUserAgent := rest.DefaultKubernetesUserAgent()
	longVersion := "v" + strings.Repeat("9", 128)
	tests := []struct {
		name      string
		version   string
		existing  string
		want      string
		forbidden []string
	}{
		{
			name:    "default version",
			version: "unknown",
			want:    defaultUserAgent + " kubectl-ome/unknown",
		},
		{
			name:     "empty version",
			version:  "",
			existing: "operations-console/2",
			want:     "operations-console/2 kubectl-ome/unknown",
		},
		{
			name:     "semantic version",
			version:  "v1.2.3",
			existing: "operations-console/2",
			want:     "operations-console/2 kubectl-ome/v1.2.3",
		},
		{
			name:    "git describe version",
			version: "v1.2.3-14-g0123abc-dirty",
			want:    defaultUserAgent + " kubectl-ome/v1.2.3-14-g0123abc-dirty",
		},
		{
			name:      "hostile version",
			version:   "v1.2.3\r\nAuthorization: Bearer PRIVATE_TOKEN",
			existing:  "operations-console/2",
			want:      "operations-console/2 kubectl-ome/unknown",
			forbidden: []string{"Authorization", "Bearer", "PRIVATE_TOKEN", "\r", "\n"},
		},
		{
			name:      "very long version",
			version:   longVersion,
			existing:  "operations-console/2",
			want:      "operations-console/2 kubectl-ome/unknown",
			forbidden: []string{longVersion},
		},
		{
			name:     "already decorated",
			version:  "v1.2.3",
			existing: "operations-console/2 kubectl-ome/v1.2.3",
			want:     "operations-console/2 kubectl-ome/v1.2.3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setGitVersion(t, tt.version)
			config := &rest.Config{UserAgent: tt.existing}
			f := &defaultFactory{rest: config}

			got, err := f.RESTConfig()
			require.NoError(t, err)
			require.Equal(t, tt.want, got.UserAgent)
			for _, forbidden := range tt.forbidden {
				require.NotContains(t, got.UserAgent, forbidden)
			}

			again, err := f.RESTConfig()
			require.NoError(t, err)
			require.Equal(t, tt.want, again.UserAgent, "repeated acquisition must not duplicate the product token")
		})
	}
}

func TestFactoryClientsSendSameProductUserAgent(t *testing.T) {
	setGitVersion(t, "v1.2.3")

	var (
		mu         sync.Mutex
		userAgents []string
		paths      = make(map[string]int)
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		mu.Lock()
		userAgents = append(userAgents, request.Header.Get("User-Agent"))
		paths[request.URL.Path]++
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		switch {
		case request.URL.Path == "/api":
			_ = json.NewEncoder(w).Encode(metav1.APIVersions{Versions: []string{"v1"}})
		case request.URL.Path == "/apis":
			_ = json.NewEncoder(w).Encode(metav1.APIGroupList{Groups: []metav1.APIGroup{{
				Name: "ome.io",
				Versions: []metav1.GroupVersionForDiscovery{{
					GroupVersion: "ome.io/v1beta1",
					Version:      "v1beta1",
				}},
				PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "ome.io/v1beta1", Version: "v1beta1"},
			}}})
		case request.URL.Path == "/apis/ome.io/v1beta1":
			_ = json.NewEncoder(w).Encode(metav1.APIResourceList{
				GroupVersion: "ome.io/v1beta1",
				APIResources: []metav1.APIResource{{Name: "servingruntimes", Kind: "ServingRuntime", Namespaced: true}},
			})
		case strings.HasSuffix(request.URL.Path, "/pods"):
			_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"PodList","metadata":{},"items":[]}`))
		case strings.Contains(request.URL.Path, "/inferenceservices/"):
			_, _ = w.Write([]byte(`{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"service","namespace":"team-a"}}`))
		case strings.Contains(request.URL.Path, "/servingruntimes/"):
			_, _ = w.Write([]byte(`{"apiVersion":"ome.io/v1beta1","kind":"ServingRuntime","metadata":{"name":"runtime","namespace":"team-a"}}`))
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	emptyKubeconfig := ""
	flags := genericclioptions.NewConfigFlags(true)
	flags.KubeConfig = &emptyKubeconfig
	flags.APIServer = &server.URL
	flags.WrapConfigFn = func(config *rest.Config) *rest.Config {
		config.UserAgent = "operations-console/2"
		return config
	}
	f := New(flags).(*defaultFactory)
	want := "operations-console/2 kubectl-ome/v1.2.3"

	config, err := f.RESTConfig()
	require.NoError(t, err)
	require.Equal(t, want, config.UserAgent)

	ome, err := f.OMEClient()
	require.NoError(t, err)
	_, err = ome.OmeV1beta1().InferenceServices("team-a").Get(context.Background(), "service", metav1.GetOptions{})
	require.NoError(t, err)

	kube, err := f.KubeClient()
	require.NoError(t, err)
	_, err = kube.CoreV1().Pods("team-a").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)

	runtimeClient, err := f.RuntimeClient()
	require.NoError(t, err)
	err = runtimeClient.Get(context.Background(), types.NamespacedName{Namespace: "team-a", Name: "runtime"}, &v1beta1.ServingRuntime{})
	require.NoError(t, err)

	actionOME, err := f.OMEClientForAction(context.Background())
	require.NoError(t, err)
	_, err = actionOME.OmeV1beta1().InferenceServices("team-a").Get(context.Background(), "service", metav1.GetOptions{})
	require.NoError(t, err)

	actionKube, err := f.KubeClientForAction(context.Background())
	require.NoError(t, err)
	_, err = actionKube.CoreV1().Pods("team-a").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)

	actionRuntime, err := f.RuntimeClientForAction(context.Background())
	require.NoError(t, err)
	err = actionRuntime.Get(context.Background(), types.NamespacedName{Namespace: "team-a", Name: "runtime"}, &v1beta1.ServingRuntime{})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(userAgents), 6)
	for _, got := range userAgents {
		require.Equal(t, want, got)
	}
	require.Equal(t, 2, paths["/apis/ome.io/v1beta1/namespaces/team-a/inferenceservices/service"])
	require.Equal(t, 2, paths["/api/v1/namespaces/team-a/pods"])
	require.Equal(t, 2, paths["/apis/ome.io/v1beta1/namespaces/team-a/servingruntimes/runtime"])
	require.Equal(t, want, config.UserAgent)
	require.Empty(t, config.ContentType, "core protobuf negotiation must stay confined to its clone")
	require.Empty(t, config.AcceptContentTypes)
	require.Equal(t, float32(50), config.QPS)
	require.Equal(t, 300, config.Burst)
}

func setGitVersion(t *testing.T, gitVersion string) {
	t.Helper()
	previous := omeversion.GitVersion
	omeversion.GitVersion = gitVersion
	t.Cleanup(func() { omeversion.GitVersion = previous })
}
