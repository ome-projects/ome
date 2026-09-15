package factory

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestActionRuntimeBackgroundDiscoveryHonorsCommandDeadline(t *testing.T) {
	var reads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api" || r.URL.Path == "/apis" {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(300 * time.Millisecond):
			}
		}
		switch r.URL.Path {
		case "/api":
			_ = json.NewEncoder(w).Encode(metav1.APIVersions{Versions: []string{"v1"}})
		case "/apis":
			_ = json.NewEncoder(w).Encode(metav1.APIGroupList{Groups: []metav1.APIGroup{{Name: "ome.io", Versions: []metav1.GroupVersionForDiscovery{{GroupVersion: "ome.io/v1beta1", Version: "v1beta1"}}, PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "ome.io/v1beta1", Version: "v1beta1"}}}})
		case "/apis/ome.io/v1beta1":
			_ = json.NewEncoder(w).Encode(metav1.APIResourceList{GroupVersion: "ome.io/v1beta1", APIResources: []metav1.APIResource{{Name: "servingruntimes", Kind: "ServingRuntime", Namespaced: true}}})
		default:
			_ = json.NewEncoder(w).Encode(&v1beta1.ServingRuntime{TypeMeta: metav1.TypeMeta{Kind: "ServingRuntime", APIVersion: "ome.io/v1beta1"}, ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: "prod"}})
		}
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "config")
	config := clientcmdapi.NewConfig()
	config.CurrentContext = "synthetic"
	config.Contexts["synthetic"] = &clientcmdapi.Context{Cluster: "local", Namespace: "prod"}
	config.Clusters["local"] = &clientcmdapi.Cluster{Server: server.URL}
	require.NoError(t, clientcmd.WriteToFile(*config, path))
	flags := genericclioptions.NewConfigFlags(true)
	flags.KubeConfig = &path
	f := New(flags).(ActionRuntimeResolver)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	client, err := f.RuntimeClientForAction(ctx)
	require.NoError(t, err)
	started := time.Now()
	err = client.Get(ctx, types.NamespacedName{Namespace: "prod", Name: "simple"}, &v1beta1.ServingRuntime{})
	require.Error(t, err)
	require.Less(t, time.Since(started), 200*time.Millisecond, "background discovery must not outlive the action context")
	require.Positive(t, reads.Load(), "exercise real discovery, not local validation")
}

func TestActionRuntimePreservesNarrowerConfiguredTimeoutDuringDiscovery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(300 * time.Millisecond):
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api" {
			_ = json.NewEncoder(w).Encode(metav1.APIVersions{})
		} else {
			_ = json.NewEncoder(w).Encode(metav1.APIGroupList{})
		}
	}))
	defer server.Close()
	config := &rest.Config{Host: server.URL, Timeout: 20 * time.Millisecond}
	f := &defaultFactory{rest: config}
	client, err := f.RuntimeClientForAction(context.Background())
	require.NoError(t, err)
	started := time.Now()
	err = client.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "simple"}, &v1beta1.ServingRuntime{})
	require.Error(t, err)
	require.Less(t, time.Since(started), 200*time.Millisecond, "the action must not lengthen an explicit request timeout")
	require.Equal(t, 20*time.Millisecond, config.Timeout)
}

type actionRoundTripFunc func(*http.Request) (*http.Response, error)

func (f actionRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestActionContextTransportPreservesNarrowerRequestCancellationAndCleanup(t *testing.T) {
	commandCtx, commandCancel := context.WithCancel(context.Background())
	defer commandCancel()
	requestCtx, requestCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer requestCancel()
	request, err := http.NewRequestWithContext(requestCtx, "GET", "http://example.invalid", nil)
	require.NoError(t, err)
	transport := actionContextTransport{ctx: commandCtx, inner: actionRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	started := time.Now()
	_, err = transport.RoundTrip(request)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), 200*time.Millisecond)
	require.NoError(t, commandCtx.Err())

	var bound context.Context
	transport.inner = actionRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		bound = r.Context()
		return &http.Response{Body: io.NopCloser(strings.NewReader("small"))}, nil
	})
	request, err = http.NewRequest("GET", "http://example.invalid", nil)
	require.NoError(t, err)
	response, err := transport.RoundTrip(request)
	require.NoError(t, err)
	require.NoError(t, bound.Err())
	require.NoError(t, response.Body.Close())
	require.ErrorIs(t, bound.Err(), context.Canceled)
	require.NoError(t, response.Body.Close(), "cleanup must be idempotent")
	require.NoError(t, commandCtx.Err())

	commandCancel()
	transport.inner = actionRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		require.ErrorIs(t, r.Context().Err(), context.Canceled)
		return nil, context.Canceled
	})
	_, err = transport.RoundTrip(request)
	require.ErrorIs(t, err, context.Canceled)

	fresh, freshCancel := context.WithCancel(context.Background())
	defer freshCancel()
	transport.ctx = fresh
	for _, failure := range []bool{false, true} {
		transport.inner = actionRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			bound = r.Context()
			if failure {
				return nil, errors.New("synthetic inner failure")
			}
			return &http.Response{}, nil
		})
		_, _ = transport.RoundTrip(request)
		require.ErrorIs(t, bound.Err(), context.Canceled, "cleanup applies to inner error and nil response body")
		require.NoError(t, fresh.Err())
	}
	transport.inner = actionRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		bound = r.Context()
		return &http.Response{Body: io.NopCloser(strings.NewReader("small"))}, nil
	})
	response, err = transport.RoundTrip(request)
	require.NoError(t, err)
	freshCancel()
	require.Eventually(t, func() bool { return errors.Is(bound.Err(), context.Canceled) }, 200*time.Millisecond, time.Millisecond)
	require.NoError(t, response.Body.Close())
}

func TestActionRuntimeClientDoesNotMutateConfigOrCacheExpiredContext(t *testing.T) {
	var calls atomic.Int32
	config := &rest.Config{Host: "https://example.invalid", Timeout: 3 * time.Second, WrapTransport: func(http.RoundTripper) http.RoundTripper {
		return actionRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			if err := r.Context().Err(); err != nil {
				return nil, err
			}
			return nil, errors.New("SYNTHETIC_TRANSPORT")
		})
	}}
	f := &defaultFactory{rest: config}
	ctx, cancel := context.WithCancel(context.Background())
	client, err := f.RuntimeClientForAction(ctx)
	require.NoError(t, err)
	require.Nil(t, f.runtime, "action client must not enter the shared read cache")
	require.Equal(t, 3*time.Second, config.Timeout)
	require.Empty(t, config.ContentType)
	require.NotNil(t, config.WrapTransport)
	err = client.Get(ctx, types.NamespacedName{Namespace: "prod", Name: "simple"}, &v1beta1.ServingRuntime{})
	require.Error(t, err)
	require.Positive(t, calls.Load(), "preserve the existing custom wrapper")
	cancel()
	_, err = f.RuntimeClientForAction(ctx)
	require.Error(t, err)
	readClient, err := f.RuntimeClient()
	require.NoError(t, err, "normal read construction must not use the expired action context")
	require.NotNil(t, readClient)
	err = readClient.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "simple"}, &v1beta1.ServingRuntime{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "SYNTHETIC_TRANSPORT", "normal read must reach the old wrapper, not the expired action context")
	_, err = (&defaultFactory{}).RuntimeClientForAction(context.Background())
	require.Error(t, err)
	var absent *defaultFactory
	_, err = absent.RuntimeClientForAction(context.Background())
	require.Error(t, err)
	require.Equal(t, 3*time.Second, config.Timeout)
}
