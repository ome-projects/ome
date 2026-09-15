package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"sigs.k8s.io/ome/pkg/cli/factory"
)

// This catches missing registration, using the workload namespace, and a
// collector that reads anything beyond the two selected ConfigMaps.
func TestRecommendationsNamedGETs(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.RequestURI())
		w.Header().Set("Content-Type", "application/json")
		cm := corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Namespace: "caretaker"}}
		switch r.URL.Path {
		case "/api/v1/namespaces/caretaker/configmaps/settings":
			cm.Name = "settings"
			cm.Data = map[string]string{"custom.yaml": "schemaVersion: 1\nrecommendationsConfigMapName: findings\n"}
		case "/api/v1/namespaces/caretaker/configmaps/findings":
			cm.Name = "findings"
			cm.Data = map[string]string{"last-cycle.json": `{"timestamp":"` + time.Now().UTC().Format(time.RFC3339) + `","mode":"recommend-only","recommendations":[{"workload":"prod/chat","component":"engine","instance":0,"policy":"defragmentation","reason":"Fragmentation","outcome":"advisory","advisoryReason":"RawDeploymentMigrationUnsupported","score":0.5}]}`}
		default:
			http.Error(w, "unexpected read", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(cm)
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	root := NewRootCmdWithFactory(factory.Static{Kube: client, NS: "workloads"}, genericiooptions.IOStreams{Out: &out, ErrOut: &out})
	root.SetArgs([]string{"admin", "recommendations", "--ome-namespace", "control", "--alfred-namespace", "caretaker", "--alfred-config-name", "settings", "--alfred-config-key", "custom.yaml", "-o", "json"})
	if err := root.Execute(); err != nil {
		t.Fatalf("recommendations: %v", err)
	}
	want := []string{"GET /api/v1/namespaces/caretaker/configmaps/settings", "GET /api/v1/namespaces/caretaker/configmaps/findings"}
	if !reflect.DeepEqual(paths, want) {
		t.Fatalf("requests = %v, want %v", paths, want)
	}
	for _, literal := range []string{`"kind": "AlfredRecommendationsReport"`, `"workload": "prod/chat"`, `"outcome": "advisory"`, `"executability": "Unverifiable"`, `"state": "Reported"`} {
		if !strings.Contains(out.String(), literal) {
			t.Errorf("missing %s in %s", literal, &out)
		}
	}
}
