package fakeapiserver_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeapiserver"
)

const eventually = 10 * time.Second

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func pod(name string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a", Labels: map[string]string{"app": name}}}
}

func replica(name string) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a"}, Spec: v1beta1.InferenceReplicaSpec{Component: v1beta1.EngineComponent}}
}

// startCache builds a controller-runtime cache against the server and
// returns it synced.
func startCache(t *testing.T, server *fakeapiserver.Server, scheme *runtime.Scheme) cache.Cache {
	t.Helper()
	c, err := cache.New(server.Config(), cache.Options{Scheme: scheme, Mapper: server.Mapper()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = c.Start(ctx) }()
	for _, obj := range []client.Object{&corev1.Pod{}, &v1beta1.InferenceReplica{}} {
		if _, err := c.GetInformer(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	syncCtx, syncCancel := context.WithTimeout(ctx, eventually)
	defer syncCancel()
	if !c.WaitForCacheSync(syncCtx) {
		t.Fatal("the cache did not sync")
	}
	return c
}

// podNames lists the pods the cache holds, by name.
func podNames(t *testing.T, c cache.Cache) map[string]*corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatal(err)
	}
	byName := map[string]*corev1.Pod{}
	for i := range pods.Items {
		byName[pods.Items[i].Name] = &pods.Items[i]
	}
	return byName
}

func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventually)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %s", what, eventually)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func verbs(requests []fakeapiserver.Request, resource string) []string {
	var out []string
	for _, r := range requests {
		if r.Resource != resource || r.Refused {
			continue
		}
		out = append(out, string(r.Verb))
	}
	return out
}

func TestCacheSeesLoadedObjectsAndStreamedEvents(t *testing.T) {
	scheme := testScheme(t)
	server := fakeapiserver.New(t, scheme)
	server.Serve(&corev1.Pod{}, meta.RESTScopeNameNamespace)
	server.Serve(&v1beta1.InferenceReplica{}, meta.RESTScopeNameNamespace)
	server.Load(pod("a"), pod("b"), pod("c"), replica("r"))

	c := startCache(t, server, scheme)
	if got := podNames(t, c); len(got) != 3 || got["a"].ResourceVersion == "" {
		t.Fatalf("listed pods = %v, want a, b, c with resource versions", got)
	}
	replicas := &v1beta1.InferenceReplicaList{}
	if err := c.List(context.Background(), replicas); err != nil || len(replicas.Items) != 1 || replicas.Items[0].Spec.Component != v1beta1.EngineComponent {
		t.Fatalf("listed replicas = %+v, %v; want the one loaded", replicas.Items, err)
	}

	server.Send(watch.Added, pod("d"))
	waitFor(t, "the added pod", func() bool { return podNames(t, c)["d"] != nil })
	changed := pod("a")
	changed.Labels["tier"] = "serving"
	server.Send(watch.Modified, changed)
	waitFor(t, "the modified pod", func() bool { return podNames(t, c)["a"].Labels["tier"] == "serving" })
	server.Send(watch.Deleted, pod("b"))
	waitFor(t, "the deleted pod", func() bool { return podNames(t, c)["b"] == nil })

	if got := verbs(server.Requests(), "pods"); strings.Join(got, ",") != "list,watch" {
		t.Fatalf("pod requests = %v, want a list then a watch", got)
	}
}

func TestInitialEventsServeTheWatchListProtocol(t *testing.T) {
	scheme := testScheme(t)
	server := fakeapiserver.New(t, scheme)
	server.InitialEvents = true
	server.Serve(&corev1.Pod{}, meta.RESTScopeNameNamespace)
	server.Serve(&v1beta1.InferenceReplica{}, meta.RESTScopeNameNamespace)
	server.Load(pod("a"), pod("b"), replica("r"))

	c := startCache(t, server, scheme)
	if got := podNames(t, c); len(got) != 2 {
		t.Fatalf("listed pods = %v, want a and b", got)
	}
	for _, resource := range []string{"pods", "inferencereplicas"} {
		if got := verbs(server.Requests(), resource); strings.Join(got, ",") != "watch-list" {
			t.Fatalf("%s requests = %v, want one watch that carried the initial events and no list", resource, got)
		}
	}
	server.Send(watch.Added, pod("c"))
	waitFor(t, "the added pod", func() bool { return podNames(t, c)["c"] != nil })
}

func get(t *testing.T, url string) *http.Response {
	t.Helper()
	resp, err := http.Get(url) //nolint:gosec // the URL is the test server's
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWireFormatFollowsTheKind(t *testing.T) {
	scheme := testScheme(t)
	server := fakeapiserver.New(t, scheme)
	server.Serve(&corev1.Pod{}, meta.RESTScopeNameNamespace)
	server.Serve(&v1beta1.InferenceReplica{}, meta.RESTScopeNameNamespace)
	server.Load(pod("a"), replica("r"))
	host := server.Config().Host

	pods := get(t, host+"/api/v1/pods?limit=500&resourceVersion=0")
	if pods.StatusCode != http.StatusOK || pods.Header.Get("Content-Type") != runtime.ContentTypeProtobuf {
		t.Fatalf("pods list: status %d, content type %q; want 200 and protobuf", pods.StatusCode, pods.Header.Get("Content-Type"))
	}
	replicas := get(t, host+"/apis/ome.io/v1beta1/namespaces/team-a/inferencereplicas")
	body, _ := io.ReadAll(replicas.Body)
	if replicas.Header.Get("Content-Type") != runtime.ContentTypeJSON || !strings.Contains(string(body), `"kind":"InferenceReplicaList"`) || !strings.Contains(string(body), `"component":"engine"`) {
		t.Fatalf("replica list: content type %q, body %s; want JSON with the loaded replica", replicas.Header.Get("Content-Type"), body)
	}
}

func TestForgetRefusesTheNextListUntilLoad(t *testing.T) {
	scheme := testScheme(t)
	server := fakeapiserver.New(t, scheme)
	server.Serve(&corev1.Pod{}, meta.RESTScopeNameNamespace)
	server.Load(pod("a"))
	host := server.Config().Host

	server.Forget(&corev1.Pod{})
	if resp := get(t, host+"/api/v1/pods"); resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("list after Forget: status %d, want 500", resp.StatusCode)
	}
	server.Load(pod("a"))
	if resp := get(t, host+"/api/v1/pods"); resp.StatusCode != http.StatusOK {
		t.Fatalf("list after Load: status %d, want 200", resp.StatusCode)
	}
	requests := server.Requests()
	if len(requests) != 2 || !requests[0].Refused || requests[1].Refused {
		t.Fatalf("requests = %+v, want the refused list then the served one", requests)
	}
}

func TestRefusesWhatItDoesNotServe(t *testing.T) {
	scheme := testScheme(t)
	server := fakeapiserver.New(t, scheme)
	server.Serve(&corev1.Pod{}, meta.RESTScopeNameNamespace)
	host := server.Config().Host
	for _, path := range []string{"/api/v1/pods/a", "/apis/apps/v1/deployments", "/api", "/apis"} {
		if resp := get(t, host+path); resp.StatusCode != http.StatusNotFound {
			t.Fatalf("GET %s: status %d, want 404", path, resp.StatusCode)
		}
	}
	empty := get(t, host+"/api/v1/pods")
	body, _ := io.ReadAll(empty.Body)
	if empty.StatusCode != http.StatusOK || len(body) == 0 {
		t.Fatalf("a served kind with nothing loaded: status %d, %d bytes; want an empty list", empty.StatusCode, len(body))
	}
}
