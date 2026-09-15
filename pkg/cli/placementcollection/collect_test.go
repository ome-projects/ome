package placementcollection

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
)

const parentWire = `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"demo","namespace":"cli-demo","uid":"fixture-parent","generation":7},"spec":{}}`

func TestNamedPrimaryAndOnlyRequestedOptionalReads(t *testing.T) {
	for _, view := range []View{Status, Explain, Endpoint} {
		t.Run(string(view), func(t *testing.T) {
			var pathsMu sync.Mutex
			var paths []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				pathsMu.Lock()
				paths = append(paths, r.Method+" "+r.URL.RequestURI())
				pathsMu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/apis/ome.io/v1beta1/namespaces/cli-demo/inferenceservices/demo":
					fmt.Fprint(w, parentWire)
				case "/apis/ome.io/v1beta1/workloadclusters":
					fmt.Fprint(w, `{"kind":"WorkloadClusterList","apiVersion":"ome.io/v1beta1","items":[]}`)
				case "/apis/ome.io/v1beta1/namespaces/cli-demo/trafficmaps/demo":
					w.WriteHeader(404)
					fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404,"details":{"name":"demo"},"message":"private-token"}`)
				default:
					t.Errorf("unexpected read: %s", r.URL)
					w.WriteHeader(500)
				}
			}))
			defer server.Close()
			c, err := versioned.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			got, err := Collect(context.Background(), c.OmeV1beta1(), "cli-demo", "demo", view)
			if err != nil {
				t.Fatal(err)
			}
			if got.InferenceService == nil || got.InferenceService.Name != "demo" {
				t.Fatalf("missing named primary: %+v", got)
			}
			wantReads := 1
			if view != Status {
				wantReads = 2
			}
			pathsMu.Lock()
			observedPaths := append([]string(nil), paths...)
			pathsMu.Unlock()
			if len(observedPaths) != wantReads {
				t.Fatalf("reads = %v, want %d", observedPaths, wantReads)
			}
			if view == Explain && (!got.Fleet.Complete || got.Fleet.Pages != 1) {
				t.Fatalf("fleet = %+v", got.Fleet)
			}
			if view == Endpoint && got.TrafficMapAcquisition.Reason != "NotFound" {
				t.Fatalf("missing optional map = %+v", got.TrafficMapAcquisition)
			}
		})
	}
}

func TestPrimaryIdentityAndErrorAreFatalAndPrivate(t *testing.T) {
	for _, body := range []string{
		strings.Replace(parentWire, `"name":"demo"`, `"name":"other"`, 1),
		strings.Replace(parentWire, `"namespace":"cli-demo"`, `"namespace":"other"`, 1),
		strings.Replace(parentWire, `"uid":"fixture-parent"`, `"uid":""`, 1),
		`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,"message":"https://secret:token@private.invalid"}`,
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if strings.Contains(body, `"code":403`) {
				w.WriteHeader(403)
			}
			fmt.Fprint(w, body)
		}))
		c, _ := versioned.NewForConfig(&rest.Config{Host: server.URL})
		got, err := Collect(context.Background(), c.OmeV1beta1(), "cli-demo", "demo", Explain)
		server.Close()
		if err == nil || got.InferenceService != nil {
			t.Fatalf("accepted invalid primary: %+v %v", got, err)
		}
		if strings.Contains(err.Error(), "token") || strings.Contains(err.Error(), "private.invalid") {
			t.Fatalf("raw error leaked: %v", err)
		}
	}
}

func TestFleetPaginationBudgetsAndOptionalFailure(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		perPage, code        int
		continuation         bool
		wantItems, wantPages int
		reason               string
	}{
		{"two-pages", 32, 200, true, 64, 2, "PageBudgetExceeded"},
		{"ignored-limit", 40, 200, false, 32, 1, "ItemBudgetExceeded"},
		{"forbidden", 0, 403, false, 0, 0, "Forbidden"},
		{"unsupported", 0, 404, false, 0, 0, "UnsupportedAPI"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reads atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if strings.HasSuffix(r.URL.Path, "inferenceservices/demo") {
					fmt.Fprint(w, parentWire)
					return
				}
				read := reads.Add(1)
				if r.URL.Query().Get("limit") != "32" {
					t.Errorf("limit = %s", r.URL.RawQuery)
				}
				if read == 2 && r.URL.Query().Get("continue") != "fixture-next" {
					t.Errorf("continuation = %s", r.URL.RawQuery)
				}
				if tc.code != 200 {
					w.WriteHeader(tc.code)
					fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"%s","code":%d,"message":"private-token"}`, map[int]string{403: "Forbidden", 404: "NotFound"}[tc.code], tc.code)
					return
				}
				items := []json.RawMessage{}
				for i := 0; i < tc.perPage; i++ {
					items = append(items, json.RawMessage(fmt.Sprintf(`{"metadata":{"name":"cluster-%d-%d"}}`, read, i)))
				}
				cont := ""
				if tc.continuation {
					cont = "fixture-next"
					if read == 2 {
						cont = "fixture-next2"
					}
				}
				json.NewEncoder(w).Encode(struct {
					Kind       string `json:"kind"`
					APIVersion string `json:"apiVersion"`
					Metadata   struct {
						Continue string `json:"continue"`
					} `json:"metadata"`
					Items []json.RawMessage `json:"items"`
				}{Kind: "WorkloadClusterList", APIVersion: "ome.io/v1beta1", Metadata: struct {
					Continue string `json:"continue"`
				}{cont}, Items: items})
			}))
			defer server.Close()
			c, _ := versioned.NewForConfig(&rest.Config{Host: server.URL})
			got, err := Collect(context.Background(), c.OmeV1beta1(), "cli-demo", "demo", Explain)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.WorkloadClusters) != tc.wantItems || got.Fleet.Pages != tc.wantPages || string(got.Fleet.Reason) != tc.reason || got.Fleet.Complete {
				t.Fatalf("fleet=%+v items=%d", got.Fleet, len(got.WorkloadClusters))
			}
		})
	}
}

func TestCancellationAndPerRequestDeadline(t *testing.T) {
	var mu sync.Mutex
	reads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reads++
		mu.Unlock()
		<-r.Context().Done()
	}))
	defer server.Close()
	c, _ := versioned.NewForConfig(&rest.Config{Host: server.URL})
	started := time.Now()
	_, err := collect(context.Background(), c.OmeV1beta1(), "cli-demo", "demo", Status, limits{time.Second, 25 * time.Millisecond})
	if err == nil || time.Since(started) > time.Second {
		t.Fatalf("deadline not respected: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = Collect(ctx, c.OmeV1beta1(), "cli-demo", "demo", Explain)
	if err == nil {
		t.Fatal("cancelled primary accepted")
	}
	mu.Lock()
	defer mu.Unlock()
	if reads != 1 {
		t.Fatalf("cancelled request performed extra reads: %d", reads)
	}
}

func TestReadDoesNotRetryRateLimitedResponse(t *testing.T) {
	var reads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reads.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"TooManyRequests","code":429,"message":"private-token"}`)
	}))
	defer server.Close()
	client, _ := versioned.NewForConfig(&rest.Config{Host: server.URL})
	_, err := Collect(context.Background(), client.OmeV1beta1(), "cli-demo", "demo", Status)
	if err == nil || reads.Load() != 1 {
		t.Fatalf("read retried: reads=%d err=%v", reads.Load(), err)
	}
}

func TestOptionalNamedGeneric404IsNotUnsupportedAPI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "inferenceservices/demo") {
			fmt.Fprint(w, parentWire)
			return
		}
		w.WriteHeader(404)
		fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
	}))
	defer server.Close()
	client, _ := versioned.NewForConfig(&rest.Config{Host: server.URL})
	r, err := Collect(context.Background(), client.OmeV1beta1(), "cli-demo", "demo", Endpoint)
	if err != nil || r.TrafficMapAcquisition.Reason != "NotFound" {
		t.Fatalf("named404=%+v err=%v", r.TrafficMapAcquisition, err)
	}
}

func TestOptionalDeadlineAndPartialPageRemainBoundedDiagnostics(t *testing.T) {
	for _, view := range []View{Explain, Endpoint} {
		t.Run(string(view), func(t *testing.T) {
			var reads atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reads.Add(1)
				w.Header().Set("Content-Type", "application/json")
				if strings.HasSuffix(r.URL.Path, "inferenceservices/demo") {
					fmt.Fprint(w, parentWire)
					return
				}
				<-r.Context().Done()
			}))
			defer server.Close()
			client, _ := versioned.NewForConfig(&rest.Config{Host: server.URL})
			r, err := collect(context.Background(), client.OmeV1beta1(), "cli-demo", "demo", view, limits{time.Second, 25 * time.Millisecond})
			acquisition := r.Fleet
			if view == Endpoint {
				acquisition = r.TrafficMapAcquisition
			}
			if err != nil || r.InferenceService == nil || reads.Load() != 2 || acquisition.State != "Unavailable" || acquisition.Reason != "Timeout" || acquisition.Complete {
				t.Fatalf("optional timeout lost primary/budget: %+v reads=%d err=%v", acquisition, reads.Load(), err)
			}
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "inferenceservices/demo") {
			fmt.Fprint(w, parentWire)
		} else if r.URL.Query().Get("continue") == "" {
			fmt.Fprint(w, `{"kind":"WorkloadClusterList","apiVersion":"ome.io/v1beta1","metadata":{"continue":"next"},"items":[{"metadata":{"name":"demo-a"}}]}`)
		} else {
			w.WriteHeader(405)
			fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"MethodNotAllowed","code":405,"message":"private-token"}`)
		}
	}))
	defer server.Close()
	client, _ := versioned.NewForConfig(&rest.Config{Host: server.URL})
	r, err := Collect(context.Background(), client.OmeV1beta1(), "cli-demo", "demo", Explain)
	if err != nil || r.Fleet.State != "Partial" || r.Fleet.Reason != "UnsupportedAPI" || r.Fleet.Complete || r.Fleet.Pages != 1 || r.Fleet.Admitted != 1 || r.Fleet.Returned != 1 {
		t.Fatalf("partial retained window mislabeled: %+v err=%v", r.Fleet, err)
	}
}
