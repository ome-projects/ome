package admin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/ome/pkg/cli/doctorcollection"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/yaml"
)

func TestDoctorProductionRedirectExitAndPrivacyContract(t *testing.T) {
	for _, status := range []int{301, 302, 303, 307, 308} {
		for _, target := range []struct{ name, path string }{
			{"required discovery", "/api/v1"},
			{"optional discovery", "/apis/keda.sh/v1alpha1"},
			{"manager", "/apis/apps/v1/namespaces/control/deployments/ome-controller-manager"},
			{"selected ISVC", "/apis/ome.io/v1beta1/namespaces/team-a/inferenceservices/chat"},
		} {
			for _, format := range []string{"table", "wide", "json", "yaml"} {
				t.Run(fmt.Sprintf("%d/%s/%s", status, target.name, format), func(t *testing.T) {
					paths := []string{}
					config := &rest.Config{Host: "https://never-contact.invalid"}
					config.WrapTransport = func(http.RoundTripper) http.RoundTripper {
						return doctorRoundTripFunc(func(req *http.Request) (*http.Response, error) {
							paths = append(paths, req.URL.Path)
							if req.Method != "GET" || req.URL.RawQuery != "" {
								t.Fatal("production constructor changed exact read request")
							}
							code := 200
							headers := http.Header{"Content-Type": {"application/json"}}
							body := doctorReadSafetyBody(req.URL.Path)
							if req.URL.Path == target.path {
								code = status
								body = "SECRET untrusted redirect body"
								headers.Set("Location", "https://never-contact.invalid/api/v1/namespaces/control/secrets/SECRET")
							}
							return &http.Response{StatusCode: code, Header: headers, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
						})
					}
					f := newDoctorFactory()
					f.config, f.Kube, f.OME = config, nil, nil
					deps := doctorDeps("complete")
					deps.collect = doctorcollection.Collect
					var out, stderr bytes.Buffer
					cmd := doctorCommand(f, &out, &stderr, deps)
					args := []string{"doctor", "--ome-namespace", "control", "-o", format}
					want := []string{"/api/v1", "/apis/apps/v1", "/apis/ome.io/v1beta1", "/apis/autoscaling/v2", "/apis/keda.sh/v1alpha1", "/apis/gateway.networking.k8s.io/v1", "/apis/kueue.x-k8s.io/v1beta1", "/apis/apps/v1/namespaces/control/deployments/ome-controller-manager"}
					wantExit := 0
					if target.name == "selected ISVC" {
						args = append(args, "--isvc", "chat")
						want = append(want, target.path)
						wantExit = 1
					}
					cmd.SetArgs(args)
					err := cmd.Execute()
					if exitcode.FromError(err) != wantExit || !reflect.DeepEqual(paths, want) || stderr.Len() != 0 {
						t.Fatalf("redirect contract: err=%v paths=%v stderr bytes=%d", err, paths, stderr.Len())
					}
					if !reflect.DeepEqual(f.calls, []string{"Namespace", "ContextName", "RESTConfig"}) {
						t.Fatal("doctor acquired cached shared factory clients")
					}
					if wantExit == 1 {
						if out.Len() != 0 || err.Error() != "doctor collection failed" {
							t.Fatal("selected redirect produced a report or unsafe error")
						}
						return
					}
					if out.Len() == 0 || strings.Contains(out.String(), "SECRET") || strings.Contains(out.String(), "https://") {
						t.Fatal("diagnostic redirect leaked Location/body or omitted report")
					}
					if format == "table" || format == "wide" {
						return
					}
					data := out.Bytes()
					if format == "yaml" {
						data, err = yaml.YAMLToJSON(data)
						if err != nil {
							t.Fatal(err)
						}
					}
					var value r.DoctorReport
					if err := json.Unmarshal(data, &value); err != nil || value.Content.HasViolations() {
						t.Fatal("redirect became a required API absence assertion")
					}
				})
			}
		}
	}
}

func doctorReadSafetyBody(path string) string {
	switch path {
	case "/api/v1":
		return `{"apiVersion":"v1","kind":"APIResourceList","groupVersion":"v1","resources":[{"name":"pods","kind":"Pod","namespaced":true},{"name":"events","kind":"Event","namespaced":true},{"name":"configmaps","kind":"ConfigMap","namespaced":true}]}`
	case "/apis/apps/v1":
		return `{"apiVersion":"v1","kind":"APIResourceList","groupVersion":"apps/v1","resources":[{"name":"deployments","kind":"Deployment","namespaced":true}]}`
	case "/apis/ome.io/v1beta1":
		return `{"apiVersion":"v1","kind":"APIResourceList","groupVersion":"ome.io/v1beta1","resources":[{"name":"inferenceservices","kind":"InferenceService","namespaced":true},{"name":"basemodels","kind":"BaseModel","namespaced":true},{"name":"servingruntimes","kind":"ServingRuntime","namespaced":true},{"name":"clusterbasemodels","kind":"ClusterBaseModel","namespaced":false},{"name":"clusterservingruntimes","kind":"ClusterServingRuntime","namespaced":false}]}`
	case "/apis/apps/v1/namespaces/control/deployments/ome-controller-manager":
		return `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"ome-controller-manager","namespace":"control","generation":123}}`
	case "/apis/ome.io/v1beta1/namespaces/team-a/inferenceservices/chat":
		return `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"chat","namespace":"team-a","uid":"fixture-uid","generation":5}}`
	default:
		return fmt.Sprintf(`{"apiVersion":"v1","kind":"APIResourceList","groupVersion":%q,"resources":[]}`, strings.TrimPrefix(path, "/apis/"))
	}
}
