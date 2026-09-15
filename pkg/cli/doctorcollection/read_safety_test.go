package doctorcollection

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	omev1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/report"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

type countedREST struct {
	rest.Interface
	gets int
}

func (c *countedREST) Get() *rest.Request {
	c.gets++
	return c.Interface.Get()
}

func TestCollectRejectsUnsupportedRESTClientsBeforeAnyRead(t *testing.T) {
	for _, slot := range []string{"discovery", "apps", "OME"} {
		t.Run(slot, func(t *testing.T) {
			clients, paths, _ := fixtureClients(t, nil)
			unsupported := &countedREST{}
			switch slot {
			case "discovery":
				unsupported.Interface, clients.discovery = clients.discovery, unsupported
			case "apps":
				unsupported.Interface, clients.apps = clients.apps, unsupported
			case "OME":
				unsupported.Interface, clients.ome = clients.ome, unsupported
			}
			_, err := Collect(context.Background(), clients, Selection{WorkloadNamespace: "team-a", OMENamespace: "control", ISVCName: "chat"}, time.Second)
			if err == nil || unsupported.gets != 0 || len(*paths) != 0 {
				t.Fatalf("unsupported shape read: err=%v Get calls=%d wire requests=%d", err, unsupported.gets, len(*paths))
			}
		})
	}
}

func TestCollectRedirectsCannotExpandExactGETPlan(t *testing.T) {
	for _, status := range []int{301, 302, 303, 307, 308} {
		for _, target := range []struct{ name, path string }{
			{"required discovery", "/api/v1"},
			{"optional discovery", "/apis/keda.sh/v1alpha1"},
			{"manager", "/apis/apps/v1/namespaces/control/deployments/ome-controller-manager"},
			{"selected ISVC", "/apis/ome.io/v1beta1/namespaces/team-a/inferenceservices/chat"},
		} {
			for _, destination := range []string{"secret path", "other path", "other host"} {
				t.Run(fmt.Sprintf("%d/%s/%s", status, target.name, destination), func(t *testing.T) {
					// Both hosts are credential-free local fixtures. No redirected
					// route is authorized, regardless of its payload or hostname.
					var redirected atomic.Int32
					other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
						redirected.Add(1)
						w.Header().Set("Content-Type", "application/json")
						json.NewEncoder(w).Encode(discoveryFixture("/api/v1"))
					}))
					t.Cleanup(other.Close)
					location := "/api/v1/namespaces/control/secrets/review-probe"
					if destination == "other path" {
						location = "/unselected-read"
					} else if destination == "other host" {
						location = other.URL + "/unselected-read"
					}
					clients, paths, mu := fixtureClients(t, func(w http.ResponseWriter, req *http.Request) bool {
						if req.URL.Path == target.path {
							w.Header().Set("Location", location)
							w.WriteHeader(status)
							return true
						}
						if req.URL.Path == location {
							redirected.Add(1)
							json.NewEncoder(w).Encode(discoveryFixture("/api/v1"))
							return true
						}
						return false
					})
					selection := Selection{WorkloadNamespace: "team-a", OMENamespace: "control"}
					want := append(append([]string{}, fixturePaths...), "/apis/apps/v1/namespaces/control/deployments/ome-controller-manager")
					if target.name == "selected ISVC" {
						selection.ISVCName = "chat"
						want = append(want, target.path)
					}
					s, err := Collect(context.Background(), clients, selection, time.Second)
					if redirected.Load() != 0 {
						t.Errorf("redirect followed %d unselected requests", redirected.Load())
					}
					mu.Lock()
					got := append([]string{}, (*paths)...)
					mu.Unlock()
					if !reflect.DeepEqual(got, want) {
						t.Errorf("wire GET plan=%v want=%v", got, want)
					}
					if target.name == "selected ISVC" {
						if err == nil || err.Error() != "selected InferenceService unavailable" {
							t.Fatalf("selected redirect not a safe primary failure: %v", err)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if (r.DoctorContent{APIs: s.APIs}).HasViolations() {
						t.Fatal("redirect falsely proved required API absence")
					}
					if target.name == "manager" {
						if s.Reads[0].Outcome != r.DoctorUnavailable || s.Reads[0].Reason != r.DoctorUnreadable {
							t.Fatalf("redirected manager=%+v", s.Reads[0])
						}
					} else {
						version := "v1"
						if target.name == "optional discovery" {
							version = "keda.sh/v1alpha1"
						}
						for _, api := range s.APIs {
							if api.GroupVersion == version && (api.Availability != r.DoctorUnavailable || api.Reason != r.DoctorUnreadable) {
								t.Fatalf("redirected discovery=%+v", api)
							}
						}
					}
				})
			}
		}
	}
}

func TestCollectManagerGenerationRequiresValidatedIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, objectName, namespace, version, kind string
		generation, wantGeneration                 int64
		malformed                                  bool
	}{
		{"wrong name", "different-deployment", "control", "apps/v1", "Deployment", 999, 0, true},
		{"wrong namespace", "ome-controller-manager", "different-namespace", "apps/v1", "Deployment", 999, 0, true},
		{"wrong kind", "ome-controller-manager", "control", "apps/v1", "StatefulSet", 999, 0, true},
		{"wrong version", "ome-controller-manager", "control", "apps/v1beta1", "Deployment", 999, 0, true},
		{"negative generation", "ome-controller-manager", "control", "apps/v1", "Deployment", -999, 0, false},
		{"validated generation", "ome-controller-manager", "control", "apps/v1", "Deployment", 123, 123, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clients, _, _ := fixtureClients(t, func(w http.ResponseWriter, req *http.Request) bool {
				if req.URL.Path != "/apis/apps/v1/namespaces/control/deployments/ome-controller-manager" {
					return false
				}
				json.NewEncoder(w).Encode(appsv1.Deployment{TypeMeta: metav1.TypeMeta{APIVersion: tc.version, Kind: tc.kind}, ObjectMeta: metav1.ObjectMeta{Name: tc.objectName, Namespace: tc.namespace, Generation: tc.generation}})
				return true
			})
			s, err := Collect(context.Background(), clients, Selection{WorkloadNamespace: "team-a", OMENamespace: "control"}, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			content := r.DoctorContent{APIs: s.APIs, Reads: s.Reads, Features: s.Features}
			value := r.DoctorReport{Envelope: r.NewEnvelope("DoctorReport", r.Metadata{Name: "doctor", Namespace: "team-a"}, content, r.ClockFunc(func() time.Time { return time.Unix(0, 0) }))}
			value.Sources = s.Sources
			value = value.Canonical()
			for _, source := range value.Sources {
				if source.Kind != "Deployment" {
					continue
				}
				if source.Name != "ome-controller-manager" || source.Namespace != "control" || source.Generation != tc.wantGeneration {
					t.Errorf("unvalidated source provenance=%+v", source)
				}
				if tc.malformed && (source.Evidence != r.EvidenceUnavailable || source.UnavailableReason != r.UnavailableMalformedPayload) {
					t.Errorf("rejected source evidence=%+v", source)
				}
			}
			for _, format := range []string{"table", "wide", "json", "yaml"} {
				var out bytes.Buffer
				if format == "wide" {
					err = value.Content.WideTable().Write(&out)
				} else {
					err = report.Write(&out, report.Format(format), value)
				}
				if err != nil {
					t.Fatal(err)
				}
				if tc.wantGeneration == 0 && strings.Contains(out.String(), "999") {
					t.Errorf("%s retained rejected generation 999", format)
				}
			}
		})
	}
}

func TestCollectSelectedISVCRejectsWrongRegisteredGVK(t *testing.T) {
	for _, tc := range []struct{ name, version, kind string }{
		{"wrong kind", "ome.io/v1beta1", "BaseModel"},
		{"wrong version", "ome.io/v99", "InferenceService"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clients, paths, _ := fixtureClients(t, func(w http.ResponseWriter, req *http.Request) bool {
				if req.URL.Path != "/apis/ome.io/v1beta1/namespaces/team-a/inferenceservices/chat" {
					return false
				}
				json.NewEncoder(w).Encode(omev1.InferenceService{TypeMeta: metav1.TypeMeta{APIVersion: tc.version, Kind: tc.kind}, ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "team-a", UID: "fixture-uid", Generation: 999}})
				return true
			})
			s, err := Collect(context.Background(), clients, Selection{WorkloadNamespace: "team-a", OMENamespace: "control", ISVCName: "chat"}, time.Second)
			if err == nil || err.Error() != "selected InferenceService unavailable" || len(*paths) != 9 {
				t.Fatalf("wrong primary GVK accepted: err=%v requests=%d", err, len(*paths))
			}
			for _, source := range s.Sources {
				if source.Kind == "InferenceService" || source.Generation == 999 {
					t.Fatal("rejected primary body contributed source evidence")
				}
			}
		})
	}
}
