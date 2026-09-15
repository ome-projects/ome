package placement

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	c "sigs.k8s.io/ome/pkg/cli/placementcollection"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func TestAllFormatsOptionalMissingRemainDiagnostic(t *testing.T) {
	for _, view := range []c.View{c.Status, c.Explain, c.Endpoint} {
		for _, format := range []string{"table", "wide", "json", "yaml"} {
			t.Run(string(view)+"/"+format, func(t *testing.T) {
				client, reads := wireClient(t, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"demo","namespace":"cli-demo","uid":"fixture-uid","generation":1}}`, 200)
				var out, stderr bytes.Buffer
				cmd := newReadCmd(factory.Static{OME: client, NS: "cli-demo"}, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, view, v.ClockFunc(func() time.Time { return time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC) }))
				cmd.SetArgs([]string{"demo", "-o", format})
				err := cmd.Execute()
				if err != nil {
					t.Fatal(err)
				}
				if exitcode.FromError(err) != 0 || stderr.Len() != 0 || out.Len() == 0 {
					t.Fatalf("result=%v stdout=%s stderr=%s", err, &out, &stderr)
				}
				if format == "json" {
					decoder := json.NewDecoder(&out)
					var doc struct {
						APIVersion string `json:"apiVersion"`
						Kind       string `json:"kind"`
					}
					if err := decoder.Decode(&doc); err != nil || doc.APIVersion != "cli.ome.io/v1alpha1" {
						t.Fatalf("JSON=%+v %v", doc, err)
					}
					if err := decoder.Decode(&doc); !errors.Is(err, io.EOF) {
						t.Fatalf("not one JSON: %v", err)
					}
				}
				wantReads := int32(2)
				if view == c.Status {
					wantReads = 1
				}
				if reads.Load() != wantReads {
					t.Fatalf("view reads=%d want%d", reads.Load(), wantReads)
				}
			})
		}
	}
}

func TestInvocationFailuresBeforeReadsAndPrivateReadError(t *testing.T) {
	for _, args := range [][]string{{}, {"one", "two"}, {"private-token\n"}, {"demo", "-o", "private-token"}, {"demo", "--watch"}, {"demo", "--confirm"}} {
		client := fake.NewSimpleClientset()
		var out bytes.Buffer
		cmd := NewCmd(factory.Static{OME: client, NS: "cli-demo"}, genericiooptions.IOStreams{Out: &out, ErrOut: &out})
		cmd.SetArgs(append([]string{"status"}, args...))
		err := cmd.Execute()
		if err == nil || exitcode.FromError(err) != 1 || len(client.Actions()) != 0 {
			t.Fatalf("invocation=%v err=%v reads=%v", args, err, client.Actions())
		}
	}
	client, reads := wireClient(t, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,"message":"https://secret:private-token@private.invalid"}`, 403)
	var out bytes.Buffer
	cmd := newReadCmd(factory.Static{OME: client, NS: "cli-demo"}, genericiooptions.IOStreams{Out: &out, ErrOut: &out}, c.Status, nil)
	cmd.SetArgs([]string{"demo"})
	err := cmd.Execute()
	if err == nil || strings.Contains(err.Error(), "private-token") || out.Len() != 0 || reads.Load() != 1 {
		t.Fatalf("raw read error/output=%v %s", err, &out)
	}
}

func wireClient(t *testing.T, body string, code int) (*versioned.Clientset, *atomic.Int32) {
	t.Helper()
	reads := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if r.Method != "GET" {
			t.Errorf("write method: %s", r.Method)
			w.WriteHeader(500)
			return
		}
		switch r.URL.Path {
		case "/apis/ome.io/v1beta1/namespaces/cli-demo/inferenceservices/demo":
			w.WriteHeader(code)
			fmt.Fprint(w, body)
		case "/apis/ome.io/v1beta1/workloadclusters":
			fmt.Fprint(w, `{"kind":"WorkloadClusterList","apiVersion":"ome.io/v1beta1","items":[]}`)
		case "/apis/ome.io/v1beta1/namespaces/cli-demo/trafficmaps/demo":
			w.WriteHeader(404)
			fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404,"details":{"name":"demo"}}`)
		default:
			t.Errorf("unexpected resource: %s", r.URL)
			w.WriteHeader(500)
		}
	}))
	t.Cleanup(server.Close)
	client, err := versioned.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	return client, reads
}

func TestHelpReadOnlyContract(t *testing.T) {
	cmd := NewCmd(factory.Static{}, genericiooptions.IOStreams{})
	if len(cmd.Commands()) != 3 {
		t.Fatal("missing family commands")
	}
	for _, sub := range cmd.Commands() {
		if !strings.HasSuffix(sub.Use, " INFERENCESERVICE") || !strings.Contains(sub.Long, "origins only") {
			t.Fatalf("help=%+v", sub)
		}
		for _, name := range []string{"watch", "confirm", "dry-run", "force", "apply", "delete"} {
			if sub.Flags().Lookup(name) != nil {
				t.Fatalf("mutation flag=%s", name)
			}
		}
	}
}
