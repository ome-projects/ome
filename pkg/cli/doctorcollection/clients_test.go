package doctorcollection

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/util/flowcontrol"
)

type clientRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn clientRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return fn(req) }

func TestNewClientsPreservesConfigurationAndOwnsHTTPPolicy(t *testing.T) {
	requests, wrappers := 0, 0
	limiter := flowcontrol.NewFakeAlwaysRateLimiter()
	config := &rest.Config{Host: "https://never-contact.invalid", Timeout: 3 * time.Second, UserAgent: "doctor-fixture", APIPath: "/unused", BearerToken: "local-test-credential", RateLimiter: limiter, Impersonate: rest.ImpersonationConfig{UserName: "fixture-user", Groups: []string{"fixture-group"}}}
	config.WrapTransport = func(http.RoundTripper) http.RoundTripper {
		wrappers++
		return clientRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests++
			if req.Method != "GET" || req.URL.Path != "/apis/apps/v1/namespaces/control/deployments/ome-controller-manager" || req.URL.RawQuery != "timeout=3s" {
				t.Fatalf("typed request changed: %s %s", req.Method, req.URL)
			}
			if req.Header.Get("Authorization") != "Bearer local-test-credential" || req.Header.Get("User-Agent") != "doctor-fixture" || req.Header.Get("Impersonate-User") != "fixture-user" || req.Header.Get("Impersonate-Group") != "fixture-group" {
				t.Fatal("configured auth, user agent or impersonation wrapper lost")
			}
			if deadline, ok := req.Context().Deadline(); !ok || time.Until(deadline) > time.Second {
				t.Fatal("bounded request context lost")
			}
			body := `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"ome-controller-manager","namespace":"control","generation":123}}`
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
		})
	}
	clients, err := NewClients(config, true)
	if err != nil {
		t.Fatal(err)
	}
	if requests != 0 || wrappers != 1 || !clients.valid(true) || clients.httpClient == http.DefaultClient || clients.httpClient.Timeout != 3*time.Second {
		t.Fatal("constructor issued a request or lost local ownership/timeout")
	}
	if err := clients.httpClient.CheckRedirect(&http.Request{}, nil); err != http.ErrUseLastResponse {
		t.Fatal("redirect policy is not reject-all")
	}
	if config.Timeout != 3*time.Second || config.APIPath != "/unused" || config.GroupVersion != nil || config.NegotiatedSerializer != nil || config.RateLimiter != limiter || config.UserAgent != "doctor-fixture" || config.Transport != nil {
		t.Fatal("shared configuration mutated")
	}
	for _, client := range []rest.Interface{clients.discovery, clients.apps, clients.ome} {
		if client.GetRateLimiter() != limiter {
			t.Fatal("configured rate limiter lost")
		}
	}
	dep := &appsv1.Deployment{}
	if reason := read(context.Background(), time.Second, clients.apps.Get().Namespace("control").Resource("deployments").Name("ome-controller-manager"), dep); reason != "" || dep.Generation != 123 || requests != 1 {
		t.Fatalf("fresh typed serializer/read: reason=%s generation=%d requests=%d", reason, dep.Generation, requests)
	}
}

func TestNewClientsDoesNotMutateDefaultHTTPClient(t *testing.T) {
	before, original := *http.DefaultClient, http.DefaultClient
	clients, err := NewClients(&rest.Config{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if clients.httpClient == original || clients.ome != nil || !clients.valid(false) || clients.valid(true) {
		t.Fatal("default HTTP client was not isolated or OME construction was not lazy")
	}
	if http.DefaultClient != original || !reflect.DeepEqual(before.Transport, original.Transport) || !reflect.DeepEqual(before.Jar, original.Jar) || before.Timeout != original.Timeout || redirectPointer(before.CheckRedirect) != redirectPointer(original.CheckRedirect) {
		t.Fatal("global default HTTP client mutated")
	}
}

func TestNewClientsDoesNotMutateCallerOwnedExecConfiguration(t *testing.T) {
	for _, selected := range []bool{false, true} {
		t.Run(fmt.Sprint(selected), func(t *testing.T) {
			original := &runtime.Unknown{Raw: []byte(`{"private":"PRIVATE_SENTINEL"}`)}
			provider := &clientcmdapi.ExecConfig{Command: "never-executed", APIVersion: "client.authentication.k8s.io/v1beta1", InteractiveMode: clientcmdapi.NeverExecInteractiveMode, Config: original}
			config := &rest.Config{Host: "https://never-contact.invalid", ExecProvider: provider}
			clients, err := NewClients(config, selected)
			if err != nil || !clients.valid(selected) {
				t.Fatalf("valid exec configuration constructor failed: %v", err)
			}
			if config.ExecProvider != provider || provider.Config != original || string(original.Raw) != `{"private":"PRIVATE_SENTINEL"}` {
				t.Fatal("constructor mutated caller-owned exec configuration")
			}
		})
	}
}

func redirectPointer(fn func(*http.Request, []*http.Request) error) uintptr {
	if fn == nil {
		return 0
	}
	return reflect.ValueOf(fn).Pointer()
}

func TestNewClientsPreservesTLSAndTransportWrapper(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(appsv1.Deployment{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}, ObjectMeta: metav1.ObjectMeta{Name: "ome-controller-manager", Namespace: "control"}})
	}))
	t.Cleanup(server.Close)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	config := &rest.Config{Host: server.URL, TLSClientConfig: rest.TLSClientConfig{CAData: ca}}
	requests := 0
	config.WrapTransport = func(next http.RoundTripper) http.RoundTripper {
		return clientRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests++
			return next.RoundTrip(req)
		})
	}
	clients, err := NewClients(config, false)
	if err != nil {
		t.Fatal(err)
	}
	if reason := read(context.Background(), time.Second, clients.apps.Get().Namespace("control").Resource("deployments").Name("ome-controller-manager"), &appsv1.Deployment{}); reason != "" || requests != 1 {
		t.Fatalf("configured TLS/wrapper lost: reason=%s requests=%d", reason, requests)
	}
	if config.Insecure || !reflect.DeepEqual(config.CAData, ca) || config.Transport != nil {
		t.Fatal("shared TLS configuration mutated")
	}
}

func TestNewClientsInvalidConfigurationFailsWithoutReads(t *testing.T) {
	for _, config := range []*rest.Config{nil, {Host: "https://SECRET.invalid/%zz"}, {Host: "https://never-contact.invalid", TLSClientConfig: rest.TLSClientConfig{CAData: []byte("SECRET invalid certificate")}}} {
		clients, err := NewClients(config, true)
		if err == nil || clients.valid(false) || strings.Contains(err.Error(), "SECRET") || strings.Contains(err.Error(), "https://") {
			t.Fatalf("unsafe invalid configuration result: %v", err)
		}
	}
}

func TestCollectRejectsUnownedOrMissingClientsBeforeAnyRead(t *testing.T) {
	for _, broken := range []string{"zero", "unowned HTTP", "nil HTTP", "nil redirect policy", "nil discovery", "nil apps", "nil OME", "typed nil"} {
		t.Run(broken, func(t *testing.T) {
			clients, paths, _ := fixtureClients(t, nil)
			switch broken {
			case "zero":
				clients = Clients{}
			case "unowned HTTP":
				isolated := *clients.httpClient
				clients.httpClient = &isolated
			case "nil HTTP":
				clients.httpClient = nil
			case "nil redirect policy":
				isolated := *clients.httpClient
				isolated.CheckRedirect = nil
				clients.httpClient = &isolated
			case "nil discovery":
				clients.discovery = nil
			case "nil apps":
				clients.apps = nil
			case "nil OME":
				clients.ome = nil
			case "typed nil":
				clients.discovery = (*rest.RESTClient)(nil)
			}
			_, err := Collect(context.Background(), clients, Selection{WorkloadNamespace: "team-a", OMENamespace: "control", ISVCName: "chat"}, time.Second)
			if err == nil || err.Error() != "doctor API clients unavailable" || len(*paths) != 0 {
				t.Fatalf("unowned client boundary: err=%v requests=%d", err, len(*paths))
			}
		})
	}
}
