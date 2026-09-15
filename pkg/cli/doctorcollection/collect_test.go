package doctorcollection

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	omev1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

var fixturePaths = []string{"/api/v1", "/apis/apps/v1", "/apis/ome.io/v1beta1", "/apis/autoscaling/v2", "/apis/keda.sh/v1alpha1", "/apis/gateway.networking.k8s.io/v1", "/apis/kueue.x-k8s.io/v1beta1"}

func discoveryFixture(path string) metav1.APIResourceList {
	gv := strings.TrimPrefix(path, "/apis/")
	if path == "/api/v1" {
		gv = "v1"
	}
	resources := map[string][]string{
		"v1":                           {"pods:Pod", "events:Event", "configmaps:ConfigMap"},
		"apps/v1":                      {"deployments:Deployment", "controllerrevisions:ControllerRevision"},
		"ome.io/v1beta1":               {"inferenceservices:InferenceService", "basemodels:BaseModel", "servingruntimes:ServingRuntime", "clusterbasemodels:ClusterBaseModel:cluster", "clusterservingruntimes:ClusterServingRuntime:cluster", "inferencereplicas:InferenceReplica", "autoscalerpolicies:AutoscalerPolicy", "trafficmaps:TrafficMap", "benchmarkjobs:BenchmarkJob", "finetunedweights:FineTunedWeight:cluster", "acceleratorclasses:AcceleratorClass:cluster", "acceleratorquotas:AcceleratorQuota:cluster", "workloadclusters:WorkloadCluster:cluster"},
		"autoscaling/v2":               {"horizontalpodautoscalers:HorizontalPodAutoscaler"},
		"keda.sh/v1alpha1":             {"scaledobjects:ScaledObject"},
		"gateway.networking.k8s.io/v1": {"httproutes:HTTPRoute", "gateways:Gateway", "gatewayclasses:GatewayClass:cluster"},
		"kueue.x-k8s.io/v1beta1":       {"workloads:Workload", "localqueues:LocalQueue", "clusterqueues:ClusterQueue:cluster"},
	}
	value := metav1.APIResourceList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "APIResourceList"}, GroupVersion: gv, APIResources: []metav1.APIResource{}}
	for _, encoded := range resources[gv] {
		p := strings.Split(encoded, ":")
		value.APIResources = append(value.APIResources, metav1.APIResource{Name: p[0], Kind: p[1], Namespaced: len(p) == 2, Verbs: []string{"create", "delete", "patch"}})
	}
	return value
}

func fixtureClients(t *testing.T, override func(http.ResponseWriter, *http.Request) bool) (Clients, *[]string, *sync.Mutex) {
	t.Helper()
	paths := []string{}
	mu := &sync.Mutex{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		paths = append(paths, req.URL.Path)
		mu.Unlock()
		if req.Method != http.MethodGet || req.URL.RawQuery != "" {
			t.Errorf("unexpected request %s %s", req.Method, req.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		if override != nil && override(w, req) {
			return
		}
		for _, path := range fixturePaths {
			if req.URL.Path == path {
				json.NewEncoder(w).Encode(discoveryFixture(path))
				return
			}
		}
		switch req.URL.Path {
		case "/apis/apps/v1/namespaces/control/deployments/ome-controller-manager":
			json.NewEncoder(w).Encode(appsv1.Deployment{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}, ObjectMeta: metav1.ObjectMeta{Name: "ome-controller-manager", Namespace: "control", UID: "SECRET", Annotations: map[string]string{"token": "SECRET"}}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "sidecar", Image: "SECRET"}, {Name: "manager", Image: "private.example:5000/repo:v1.2.3", Env: []corev1.EnvVar{{Name: "TOKEN", Value: "SECRET"}}}}}}}})
		case "/apis/ome.io/v1beta1/namespaces/team-a/inferenceservices/chat":
			json.NewEncoder(w).Encode(omev1.InferenceService{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"}, ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "team-a", UID: "SECRET", Generation: 9}, Status: omev1.InferenceServiceStatus{Traffic: &omev1.TrafficStatus{Algorithm: "SECRET"}}})
		default:
			t.Errorf("unexpected path %s", req.URL.Path)
			http.Error(w, "SECRET", 404)
		}
	}))
	t.Cleanup(server.Close)
	cfg := &rest.Config{Host: server.URL}
	clients, err := NewClients(cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	return clients, &paths, mu
}

func TestCollectFixedGETsAndNoInventedWorkloadPermissions(t *testing.T) {
	for _, selected := range []string{"", "chat"} {
		t.Run(selected, func(t *testing.T) {
			clients, paths, _ := fixtureClients(t, nil)
			s, err := Collect(context.Background(), clients, Selection{ContextName: "test", WorkloadNamespace: "team-a", OMENamespace: "control", ISVCName: selected}, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			want := append(append([]string{}, fixturePaths...), "/apis/apps/v1/namespaces/control/deployments/ome-controller-manager")
			if selected != "" {
				want = append(want, "/apis/ome.io/v1beta1/namespaces/team-a/inferenceservices/chat")
			}
			if !reflect.DeepEqual(*paths, want) {
				t.Fatalf("paths=%v want=%v", *paths, want)
			}
			if len(s.APIs) != 26 {
				t.Fatalf("API rows=%d", len(s.APIs))
			}
			for _, a := range s.APIs {
				if a.Availability != r.DoctorAvailable {
					t.Fatalf("not available %+v", a)
				}
			}
			if s.Manager.ImageVersionCandidate != "v1.2.3" {
				t.Fatalf("manager=%+v", s.Manager)
			}
			if len(s.Reads) != 2 || s.Reads[1].Outcome != r.DoctorNotRequested && selected == "" {
				t.Fatalf("read claims=%+v", s.Reads)
			}
			data, _ := json.Marshal(s)
			for _, bad := range []string{"SECRET", "private.example", "delete", "create", "patch", "Env"} {
				if strings.Contains(string(data), bad) {
					t.Fatalf("snapshot leaked %s: %s", bad, data)
				}
			}
		})
	}
}

func TestCollectDiscoveryErrorsAreSafeAndNeverRetried(t *testing.T) {
	for _, tc := range []struct {
		code int
		want r.DoctorReason
	}{{404, r.DoctorNotFound}, {403, r.DoctorForbidden}, {401, r.DoctorUnauthorized}, {429, r.DoctorThrottled}, {500, r.DoctorServerError}} {
		t.Run(http.StatusText(tc.code), func(t *testing.T) {
			clients, paths, _ := fixtureClients(t, func(w http.ResponseWriter, req *http.Request) bool {
				if req.URL.Path != "/apis/keda.sh/v1alpha1" {
					return false
				}
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(tc.code)
				json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Code: int32(tc.code), Reason: metav1.StatusReason(http.StatusText(tc.code)), Message: "SECRET https://private.example/token"})
				return true
			})
			s, err := Collect(context.Background(), clients, Selection{WorkloadNamespace: "team-a", OMENamespace: "control"}, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if len(*paths) != 8 {
				t.Fatalf("retry/fanout=%v", *paths)
			}
			for _, a := range s.APIs {
				if a.Resource == "scaledobjects" && a.Reason != tc.want {
					t.Fatalf("got %+v want %s", a, tc.want)
				}
			}
			data, _ := json.Marshal(s)
			if strings.Contains(string(data), "SECRET") || strings.Contains(string(data), "https://") {
				t.Fatal("error text leaked")
			}
		})
	}
}

func TestCollectFeatureGenerationAlwaysUnverifiable(t *testing.T) {
	for _, observed := range []int64{0, 8, 9, 10} {
		t.Run(string(rune('a'+observed)), func(t *testing.T) {
			clients, _, _ := fixtureClients(t, func(w http.ResponseWriter, req *http.Request) bool {
				if !strings.HasSuffix(req.URL.Path, "/inferenceservices/chat") {
					return false
				}
				json.NewEncoder(w).Encode(omev1.InferenceService{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"}, ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "team-a", UID: "uid", Generation: 9}, Status: omev1.InferenceServiceStatus{Status: duckv1.Status{ObservedGeneration: observed}, Components: map[omev1.ComponentType]omev1.ComponentStatusSpec{omev1.EngineComponent: {Lifecycle: &omev1.LifecycleStatus{}}, "SECRET": {Autoscaler: &omev1.ComponentAutoscalerStatus{}}}, Traffic: &omev1.TrafficStatus{}}})
				return true
			})
			s, err := Collect(context.Background(), clients, Selection{WorkloadNamespace: "team-a", OMENamespace: "control", ISVCName: "chat"}, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			for _, f := range s.Features {
				if f.Freshness != r.DoctorUnverifiable {
					t.Fatalf("observed=%d feature=%+v", observed, f)
				}
				if f.ID == "Autoscaling" && f.Availability != r.DoctorAbsent {
					t.Fatal("trusted arbitrary component key")
				}
			}
		})
	}
}

func TestCollectValidationPrimaryErrorsAndCancellation(t *testing.T) {
	clients, paths, _ := fixtureClients(t, nil)
	for _, tc := range []struct {
		s       Selection
		timeout time.Duration
	}{{Selection{WorkloadNamespace: "../SECRET", OMENamespace: "control"}, time.Second}, {Selection{WorkloadNamespace: "team-a", OMENamespace: "control", ISVCName: "../SECRET"}, time.Second}, {Selection{WorkloadNamespace: "team-a", OMENamespace: "control"}, 11 * time.Second}} {
		if _, err := Collect(context.Background(), clients, tc.s, tc.timeout); err == nil || strings.Contains(err.Error(), "SECRET") {
			t.Fatalf("validation=%v", err)
		}
	}
	if len(*paths) != 0 {
		t.Fatalf("validation contacted API: %v", *paths)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Collect(ctx, clients, Selection{WorkloadNamespace: "team-a", OMENamespace: "control"}, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	for _, code := range []int{404, 403, 401, 500} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			c, _, _ := fixtureClients(t, func(w http.ResponseWriter, req *http.Request) bool {
				if !strings.HasSuffix(req.URL.Path, "/inferenceservices/chat") {
					return false
				}
				w.WriteHeader(code)
				json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Code: int32(code), Status: "Failure", Message: "SECRET"})
				return true
			})
			_, err := Collect(context.Background(), c, Selection{WorkloadNamespace: "team-a", OMENamespace: "control", ISVCName: "chat"}, time.Second)
			if err == nil || strings.Contains(err.Error(), "SECRET") {
				t.Fatalf("primary=%v", err)
			}
		})
	}
}

func TestCollectPerRequestTimeoutAndParentCancellation(t *testing.T) {
	clients, paths, mu := fixtureClients(t, func(w http.ResponseWriter, req *http.Request) bool {
		if req.URL.Path != "/apis/keda.sh/v1alpha1" {
			return false
		}
		<-req.Context().Done()
		return true
	})
	s, err := Collect(context.Background(), clients, Selection{WorkloadNamespace: "team-a", OMENamespace: "control"}, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range s.APIs {
		if a.Resource == "scaledobjects" && a.Reason != r.DoctorTimeout {
			t.Fatalf("timeout=%+v", a)
		}
	}
	mu.Lock()
	count := len(*paths)
	mu.Unlock()
	if count != 8 {
		t.Fatalf("timeout requests=%d", count)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = Collect(ctx, clients, Selection{WorkloadNamespace: "team-a", OMENamespace: "control"}, time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("parent=%v", err)
	}
}

func TestCollectSelectedObjectIdentityIsRequired(t *testing.T) {
	for _, tc := range []struct{ name, namespace, uid string }{{"other", "team-a", "uid"}, {"chat", "other", "uid"}, {"chat", "team-a", ""}} {
		t.Run(tc.name+tc.namespace+tc.uid, func(t *testing.T) {
			clients, _, _ := fixtureClients(t, func(w http.ResponseWriter, req *http.Request) bool {
				if !strings.HasSuffix(req.URL.Path, "/inferenceservices/chat") {
					return false
				}
				json.NewEncoder(w).Encode(omev1.InferenceService{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"}, ObjectMeta: metav1.ObjectMeta{Name: tc.name, Namespace: tc.namespace, UID: types.UID(tc.uid)}})
				return true
			})
			if _, err := Collect(context.Background(), clients, Selection{WorkloadNamespace: "team-a", OMENamespace: "control", ISVCName: "chat"}, time.Second); err == nil {
				t.Fatal("accepted wrong selected object identity")
			}
		})
	}
}

func TestCollectDecodedDiscoveryLimitIncludesItsBoundary(t *testing.T) {
	clients, _, _ := fixtureClients(t, func(w http.ResponseWriter, req *http.Request) bool {
		if req.URL.Path != "/apis/ome.io/v1beta1" {
			return false
		}
		value := discoveryFixture(req.URL.Path)
		for len(value.APIResources) < 256 {
			value.APIResources = append(value.APIResources, metav1.APIResource{Name: "ignored", Kind: "Other"})
		}
		json.NewEncoder(w).Encode(value)
		return true
	})
	s, err := Collect(context.Background(), clients, Selection{WorkloadNamespace: "team-a", OMENamespace: "control"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range s.APIs {
		if a.Resource == "inferenceservices" && a.Availability != r.DoctorAvailable {
			t.Fatalf("rejected limit boundary: %+v", a)
		}
	}
}

func TestCollectCredentialShapedIdentitiesStayExactForPrivateGETs(t *testing.T) {
	// These are legal DNS identities; public redaction must not change requests
	// or the returned object's exact identity checks.
	name := "sk-aaaaaaaaaaaaaaaaaaaa"
	workload := "sk-bbbbbbbbbbbbbbbbbbbb"
	ome := "sk-cccccccccccccccccccc"
	clients, paths, _ := fixtureClients(t, func(w http.ResponseWriter, req *http.Request) bool {
		switch req.URL.Path {
		case "/apis/apps/v1/namespaces/sk-cccccccccccccccccccc/deployments/ome-controller-manager":
			json.NewEncoder(w).Encode(appsv1.Deployment{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}, ObjectMeta: metav1.ObjectMeta{Name: "ome-controller-manager", Namespace: ome}})
			return true
		case "/apis/ome.io/v1beta1/namespaces/sk-bbbbbbbbbbbbbbbbbbbb/inferenceservices/sk-aaaaaaaaaaaaaaaaaaaa":
			json.NewEncoder(w).Encode(omev1.InferenceService{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"}, ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: workload, UID: "uid"}})
			return true
		default:
			return false
		}
	})
	s, err := Collect(context.Background(), clients, Selection{ContextName: "local", WorkloadNamespace: workload, OMENamespace: ome, ISVCName: name}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]string{}, fixturePaths...), "/apis/apps/v1/namespaces/sk-cccccccccccccccccccc/deployments/ome-controller-manager", "/apis/ome.io/v1beta1/namespaces/sk-bbbbbbbbbbbbbbbbbbbb/inferenceservices/sk-aaaaaaaaaaaaaaaaaaaa")
	if !reflect.DeepEqual(*paths, want) {
		t.Fatalf("GET identities changed: %v", *paths)
	}
	if s.Selection.ISVCName != name || s.Selection.WorkloadNamespace != workload || s.Selection.OMENamespace != ome {
		t.Fatal("private selected identities were redacted before validation")
	}
	if s.Reads[1].Outcome != r.DoctorAvailable {
		t.Fatal("exact private GET identity validation failed")
	}
}
