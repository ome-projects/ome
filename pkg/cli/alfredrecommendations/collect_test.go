package alfredrecommendations

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"sigs.k8s.io/ome/pkg/cli/namespace"
)

func selected() namespace.Resolved {
	return namespace.Resolved{AlfredNamespace: "ome", AlfredConfigName: "alfred-config", AlfredConfigKey: "config.yaml"}
}

// Real HTTP proves the verb, exact scope, order, and returned identity checks.
func TestCollectNamedSources(t *testing.T) {
	for _, tc := range []struct {
		name, config, record, configState, recordState    string
		configStatus, recordStatus, requests              int
		wrongIdentity, configKeyMissing, recordKeyMissing bool
	}{
		{name: "default", config: "schemaVersion: 1", record: cycle("null"), configState: "Available", recordState: "Available", requests: 2},
		{name: "disabled", config: "schemaVersion: 1\nrecommendationsConfigMapEnabled: false", configState: "Available", recordState: "Disabled", requests: 1},
		{name: "config 404", configStatus: 404, configState: "NotFound", recordState: "NotRead", requests: 1},
		{name: "config forbidden", configStatus: 403, configState: "Forbidden", recordState: "NotRead", requests: 1},
		{name: "config unauthorized", configStatus: 401, configState: "Unreadable", recordState: "NotRead", requests: 1},
		{name: "config key absent", configKeyMissing: true, configState: "KeyAbsent", recordState: "NotRead", requests: 1},
		{name: "config malformed", config: "[SECRET", configState: "Malformed", recordState: "NotRead", requests: 1},
		{name: "record 404", config: "schemaVersion: 1", recordStatus: 404, configState: "Available", recordState: "NotFound", requests: 2},
		{name: "record forbidden", config: "schemaVersion: 1", recordStatus: 403, configState: "Available", recordState: "Forbidden", requests: 2},
		{name: "record unreadable", config: "schemaVersion: 1", recordStatus: 401, configState: "Available", recordState: "Unreadable", requests: 2},
		{name: "record key absent", config: "schemaVersion: 1", recordKeyMissing: true, configState: "Available", recordState: "KeyAbsent", requests: 2},
		{name: "identity", config: "schemaVersion: 1", wrongIdentity: true, configState: "IdentityMismatch", recordState: "NotRead", requests: 1},
		{name: "record oversized", config: "schemaVersion: 1", record: strings.Repeat("x", MaxRecordBytes+1), configState: "Available", recordState: "Oversized", requests: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var paths []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				paths = append(paths, r.Method+" "+r.URL.RequestURI())
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				name := "alfred-config"
				status := tc.configStatus
				data := map[string]string{"config.yaml": tc.config}
				if tc.configKeyMissing {
					data = nil
				}
				if strings.HasSuffix(r.URL.Path, "/alfred-recommendations") {
					name = "alfred-recommendations"
					status = tc.recordStatus
					data = map[string]string{RecordKey: tc.record}
					if tc.recordKeyMissing {
						data = nil
					}
				}
				if status != 0 {
					w.WriteHeader(status)
					_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Message: "password=SECRET\n\x1b[31m", Code: int32(status)})
					return
				}
				ns := "ome"
				if tc.wrongIdentity {
					ns = "elsewhere"
				}
				_ = json.NewEncoder(w).Encode(corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Annotations: map[string]string{"password": "SECRET"}}, Data: data, BinaryData: map[string][]byte{"token": []byte("SECRET")}})
			}))
			defer server.Close()
			client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			got, err := Collect(context.Background(), client.CoreV1(), selected(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if got.Config.State != tc.configState || got.RecordState != tc.recordState {
				t.Fatalf("snapshot = %+v", got)
			}
			want := []string{"GET /api/v1/namespaces/ome/configmaps/alfred-config"}
			if tc.requests == 2 {
				want = append(want, "GET /api/v1/namespaces/ome/configmaps/alfred-recommendations")
			}
			mu.Lock()
			defer mu.Unlock()
			if !reflect.DeepEqual(paths, want) {
				t.Fatalf("requests = %v; want %v", paths, want)
			}
			b, _ := json.Marshal(Project(got, nil))
			if strings.Contains(string(b), "SECRET") {
				t.Fatalf("report leaked %s", b)
			}
		})
	}
}

func TestCollectValidationCancellationAndTimeout(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); <-r.Context().Done() }))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	for _, ns := range []namespace.Resolved{{}, {AlfredNamespace: "bad/SECRET", AlfredConfigName: "config", AlfredConfigKey: "config.yaml"}, {AlfredNamespace: "ome", AlfredConfigName: "..", AlfredConfigKey: "config.yaml"}, {AlfredNamespace: "ome", AlfredConfigName: "config", AlfredConfigKey: "../secret"}} {
		if _, err := Collect(context.Background(), client.CoreV1(), ns, time.Second); err == nil || strings.Contains(err.Error(), "SECRET") {
			t.Fatalf("validation = %v", err)
		}
	}
	for _, timeout := range []time.Duration{0, -time.Second, 11 * time.Second} {
		if _, err := Collect(context.Background(), client.CoreV1(), selected(), timeout); err == nil {
			t.Fatal("invalid timeout accepted")
		}
	}
	if _, err := Collect(context.Background(), nil, selected(), time.Second); err == nil {
		t.Fatal("nil client accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Collect(ctx, client.CoreV1(), selected(), time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation = %v", err)
	}
	if hits.Load() != 0 {
		t.Fatal("invalid input caused I/O")
	}
	start := time.Now()
	got, err := Collect(context.Background(), client.CoreV1(), selected(), 25*time.Millisecond)
	if err != nil || got.Config.State != "Unreadable" || time.Since(start) > time.Second {
		t.Fatalf("timeout = %+v, %v", got, err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err := Collect(ctx, client.CoreV1(), selected(), time.Second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("parent timeout = %v", err)
	}
}

func TestCollectCancellationDuringRecordPreservesConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/alfred-config") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"namespace":"ome","name":"alfred-config"},"data":{"config.yaml":"schemaVersion: 1"}}`))
			return
		}
		cancel()
		<-r.Context().Done()
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	got, err := Collect(ctx, client.CoreV1(), selected(), time.Second)
	if !errors.Is(err, context.Canceled) || got.Config.State != "Available" {
		t.Fatalf("partial canceled = %+v, %v", got, err)
	}
}
