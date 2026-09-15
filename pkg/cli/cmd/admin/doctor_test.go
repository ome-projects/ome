package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	omev1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/doctorcollection"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/printers"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/version"
)

type doctorFactory struct {
	factory.Static
	calls  []string
	fail   string
	config *rest.Config
}

func newDoctorFactory() *doctorFactory {
	return &doctorFactory{Static: factory.Static{NS: "team-a", Context: "local", Kube: kubefake.NewClientset(), OME: omefake.NewSimpleClientset()}, config: &rest.Config{}}
}

func (f *doctorFactory) called(method string) error {
	f.calls = append(f.calls, method)
	if f.fail == method {
		return errors.New("sk-proj-0123456789abcdefghijklmnopqrstuvwxyz https://private.example/SECRET\n\x1b[2J")
	}
	return nil
}
func (f *doctorFactory) Namespace() (string, bool, error)  { return f.NS, false, f.called("Namespace") }
func (f *doctorFactory) ContextName() (string, error)      { return f.Context, f.called("ContextName") }
func (f *doctorFactory) RESTConfig() (*rest.Config, error) { return f.config, f.called("RESTConfig") }
func (f *doctorFactory) KubeClient() (kubernetes.Interface, error) {
	return f.Kube, f.called("KubeClient")
}
func (f *doctorFactory) OMEClient() (omeclient.Interface, error) { return f.OME, f.called("OMEClient") }
func (f *doctorFactory) RuntimeClient() (ctrlclient.Client, error) {
	return nil, f.called("RuntimeClient")
}

type doctorContextlessFactory struct{ factory.Factory }

func doctorCommand(f factory.Factory, out io.Writer, errOut io.Writer, deps doctorDependencies) *cobra.Command {
	parent := &cobra.Command{Use: "admin", SilenceErrors: true, SilenceUsage: true}
	ns := namespace.NewOptions()
	ns.AddFlags(parent.PersistentFlags())
	genericclioptions.NewConfigFlags(true).AddFlags(parent.PersistentFlags())
	parent.SetOut(out)
	parent.SetErr(errOut)
	parent.AddCommand(newDoctorCmd(f, genericiooptions.IOStreams{Out: out, ErrOut: errOut}, ns, deps))
	return parent
}

func doctorSnapshot(selected doctorcollection.Selection, state string) doctorcollection.Snapshot {
	s := doctorcollection.Snapshot{
		Selection: selected,
		APIs:      []r.DoctorAPI{{ID: "ome.io/v1beta1/inferenceservices", Availability: r.DoctorAvailable}, {ID: "ome.io/v1beta1/inferencereplicas", Availability: r.DoctorAvailable}},
		Reads:     []r.DoctorRead{{ID: r.DoctorReadManager, Namespace: selected.OMENamespace, Outcome: r.DoctorAvailable}, {ID: r.DoctorReadISVC, Namespace: selected.WorkloadNamespace, Name: selected.ISVCName, Outcome: r.DoctorNotRequested}},
		Manager:   r.DoctorManagerEvidence{ImageState: r.DoctorSelectedStableTag, ImageVersionCandidate: "v1.2.3"},
		Features:  []r.DoctorFeature{{ID: "Traffic", Availability: r.DoctorNotSelected}, {ID: "Lifecycle", Availability: r.DoctorNotSelected}},
		Sources:   []r.SourceReference{{Kind: "Deployment", Name: "ome-controller-manager", Namespace: selected.OMENamespace, Evidence: r.EvidenceObserved, Generation: 1}},
	}
	if selected.ISVCName != "" {
		s.Reads[1].Outcome = r.DoctorAvailable
		s.Features[0].Availability, s.Features[1].Availability = r.DoctorPresent, r.DoctorPresent
		s.Sources = append(s.Sources, r.SourceReference{Kind: "InferenceService", Name: selected.ISVCName, Namespace: selected.WorkloadNamespace, Evidence: r.EvidenceObserved, Generation: 9})
	}
	if state == "incomplete" {
		s.APIs[1].Availability, s.APIs[1].Reason = r.DoctorUnavailable, r.DoctorForbidden
		s.Warnings = []r.Warning{{Code: r.WarningSourceUnavailable, Message: "SECRET"}}
	}
	if state == "violations" {
		s.APIs[0].Availability = r.DoctorNotDiscoverable
	}
	if state == "required unreadable" {
		s.APIs[0].Availability, s.APIs[0].Reason = r.DoctorUnavailable, r.DoctorForbidden
	}
	if state == "optional missing" {
		s.APIs[1].Availability = r.DoctorNotDiscoverable
	}
	return s
}

func doctorDeps(state string) doctorDependencies {
	return doctorDependencies{clock: r.ClockFunc(func() time.Time { return time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC) }), collect: func(_ context.Context, _ doctorcollection.Clients, selected doctorcollection.Selection, _ time.Duration) (doctorcollection.Snapshot, error) {
		return doctorSnapshot(selected, state), nil
	}}
}

func TestDoctorLocalValidationBeforeAnyFactoryAcquisition(t *testing.T) {
	// Removing local validation or the closed Cobra flag diagnostic would
	// contact configuration/client seams or expose the literal private input.
	const private = "sk-proj-0123456789abcdefghijklmnopqrstuvwxyz"
	for _, args := range [][]string{
		{"extra-" + private}, {"-o", private}, {"--output="}, {"--isvc="}, {"--isvc", "../" + private},
		{"--ome-namespace="}, {"--ome-namespace", "../" + private}, {"--namespace="}, {"-n", "../" + private},
		{"--request-timeout", private}, {"--request-timeout", "-1s"}, {"--unknown-" + private + "=value"},
		{"--output"}, {"--context"}, {"--insecure-skip-tls-verify=" + private},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			f := newDoctorFactory()
			var out, errOut bytes.Buffer
			cmd := doctorCommand(f, &out, &errOut, doctorDeps("complete"))
			cmd.SetArgs(append([]string{"doctor"}, args...))
			err := cmd.Execute()
			if err == nil || exitcode.FromError(err) != 1 || len(f.calls) != 0 || out.Len() != 0 || errOut.Len() != 0 {
				t.Fatalf("local failure acquired/printed: err=%v calls=%v stdout=%s stderr=%s", err, f.calls, &out, &errOut)
			}
			if strings.Contains(err.Error(), private) || strings.ContainsAny(err.Error(), "\n\x1b") {
				t.Fatal("diagnostic leaked input")
			}
		})
	}
}

func TestDoctorSelectionTimeoutAndLazyOMEClient(t *testing.T) {
	for _, selected := range []string{"", "sk-aaaaaaaaaaaaaaaaaaaa"} {
		for _, configured := range []time.Duration{0, 3 * time.Second, 20 * time.Second} {
			f := newDoctorFactory()
			f.NS, f.Context, f.config.Timeout = "sk-bbbbbbbbbbbbbbbbbbbb", "https://private.example/SECRET", configured
			deps := doctorDeps("complete")
			seen := false
			deps.collect = func(ctx context.Context, _ doctorcollection.Clients, got doctorcollection.Selection, timeout time.Duration) (doctorcollection.Snapshot, error) {
				seen = true
				want := doctorcollection.Selection{ContextName: f.Context, WorkloadNamespace: f.NS, OMENamespace: "sk-cccccccccccccccccccc", ISVCName: selected}
				if got != want {
					t.Fatalf("private selection changed: %+v", got)
				}
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 30*time.Second {
					t.Fatal("missing whole command deadline")
				}
				wantTimeout := 10 * time.Second
				if configured == 3*time.Second {
					wantTimeout = configured
				}
				if timeout != wantTimeout || f.config.Timeout != configured {
					t.Fatal("timeout not preserved or shared config mutated")
				}
				return doctorSnapshot(got, "complete"), nil
			}
			var out bytes.Buffer
			cmd := doctorCommand(f, &out, &bytes.Buffer{}, deps)
			args := []string{"doctor", "--ome-namespace", "sk-cccccccccccccccccccc", "-o", "json"}
			if selected != "" {
				args = append(args, "--isvc", selected)
			}
			cmd.SetArgs(args)
			if err := cmd.Execute(); err != nil || !seen {
				t.Fatalf("selection failed: %v", err)
			}
			wantCalls := []string{"Namespace", "ContextName", "RESTConfig"}
			if !reflect.DeepEqual(f.calls, wantCalls) {
				t.Fatalf("unexpected acquisition: %v", f.calls)
			}
			for _, private := range []string{selected, f.NS, "sk-cccccccccccccccccccc", "https://", "SECRET"} {
				if private != "" && strings.Contains(out.String(), private) {
					t.Fatal("public identity leaked")
				}
			}
		}
	}
}

func TestDoctorPrimaryFailuresAreSafeAndProduceNoReport(t *testing.T) {
	for _, fail := range []string{"Namespace", "ContextName", "RESTConfig", "collection", "contextless", "empty namespace", "empty context", "nil config", "invalid host", "invalid TLS"} {
		t.Run(fail, func(t *testing.T) {
			f := newDoctorFactory()
			f.fail = fail
			var injected factory.Factory = f
			deps := doctorDeps("complete")
			switch fail {
			case "collection":
				deps.collect = func(context.Context, doctorcollection.Clients, doctorcollection.Selection, time.Duration) (doctorcollection.Snapshot, error) {
					return doctorcollection.Snapshot{}, errors.New("SECRET https://private.example")
				}
			case "contextless":
				injected = doctorContextlessFactory{Factory: f}
			case "empty namespace":
				f.NS = ""
			case "empty context":
				f.Context = ""
			case "nil config":
				f.config = nil
			case "invalid host":
				f.config.Host = "https://SECRET.invalid/%zz"
			case "invalid TLS":
				f.config.Host = "https://never-contact.invalid"
				f.config.CAData = []byte("SECRET invalid certificate")
			}
			var out, errOut bytes.Buffer
			cmd := doctorCommand(injected, &out, &errOut, deps)
			cmd.SetArgs([]string{"doctor", "--isvc", "chat", "-o", "json"})
			err := cmd.Execute()
			if err == nil || exitcode.FromError(err) != 1 || out.Len() != 0 || errOut.Len() != 0 {
				t.Fatalf("primary failure = %v %s", err, &out)
			}
			if strings.Contains(err.Error(), "SECRET") || strings.Contains(err.Error(), "https://") || strings.Contains(err.Error(), "sk-") {
				t.Fatal("raw failure leaked")
			}
		})
	}
}

type doctorBadWriter struct{ short bool }

func (w doctorBadWriter) Write(p []byte) (int, error) {
	if w.short {
		return len(p) - 1, nil
	}
	return 0, errors.New("SECRET https://private.example sk-aaaaaaaaaaaaaaaaaaaa")
}

func TestDoctorRenderPrecedesRequiredAPIAssertion(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		for _, short := range []bool{false, true} {
			cmd := doctorCommand(newDoctorFactory(), doctorBadWriter{short: short}, &bytes.Buffer{}, doctorDeps("violations"))
			cmd.SetArgs([]string{"doctor", "-o", format})
			err := cmd.Execute()
			if err == nil || exitcode.FromError(err) != 1 || err.Error() != "write doctor report failed" {
				t.Fatalf("writer precedence %s: %v", format, err)
			}
		}
	}
}

func TestDoctorCancellationNeverProducesCompletedEvidence(t *testing.T) {
	for _, during := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		f := newDoctorFactory()
		deps := doctorDeps("complete")
		if during {
			deps.collect = func(context.Context, doctorcollection.Clients, doctorcollection.Selection, time.Duration) (doctorcollection.Snapshot, error) {
				cancel()
				return doctorcollection.Snapshot{}, nil
			}
		} else {
			cancel()
		}
		var out bytes.Buffer
		cmd := doctorCommand(f, &out, &bytes.Buffer{}, deps)
		cmd.SetArgs([]string{"doctor", "-o", "json"})
		if err := cmd.ExecuteContext(ctx); err == nil || exitcode.FromError(err) != 1 || out.Len() != 0 {
			t.Fatalf("canceled evidence: %v %s", err, &out)
		}
		if !during && len(f.calls) != 0 {
			t.Fatal("canceled command acquired clients")
		}
	}
}

func TestDoctorFormatsPrivacyAndExitContract(t *testing.T) {
	for _, state := range []string{"complete", "incomplete", "required unreadable", "optional missing", "violations"} {
		var structured []r.DoctorReport
		for _, format := range []string{"table", "wide", "json", "yaml"} {
			f := newDoctorFactory()
			f.Context = "token=SECRETSECRET\n\x1b[2J"
			deps := doctorDeps(state)
			base := deps.collect
			deps.collect = func(ctx context.Context, clients doctorcollection.Clients, selected doctorcollection.Selection, timeout time.Duration) (doctorcollection.Snapshot, error) {
				s, err := base(ctx, clients, selected, timeout)
				s.APIs = append(s.APIs, r.DoctorAPI{ID: "SECRET", Availability: "SECRET"})
				s.Features = append(s.Features, r.DoctorFeature{ID: "SECRET", Availability: "SECRET", Freshness: "Current"})
				s.Sources = append(s.Sources, r.SourceReference{Kind: "SECRET", Name: "SECRET"})
				return s, err
			}
			var out, errOut bytes.Buffer
			cmd := doctorCommand(f, &out, &errOut, deps)
			cmd.SetArgs([]string{"doctor", "--isvc", "chat", "--ome-namespace", "control", "-o", format})
			err := cmd.Execute()
			wantCode := 0
			if state == "violations" {
				wantCode = 2
			}
			if exitcode.FromError(err) != wantCode || errOut.Len() != 0 {
				t.Fatalf("%s/%s exit=%v", state, format, err)
			}
			if state == "violations" && err.Error() != "doctor found required API violations" {
				t.Fatal("unsafe assertion error")
			}
			for _, bad := range []string{"SECRET", "https://", "\x1b", "\"generationFreshness\": \"Current\"", "generationFreshness: Current", "Healthy"} {
				if strings.Contains(out.String(), bad) {
					t.Fatalf("%s leaked %q", format, bad)
				}
			}
			if format == "table" {
				for _, line := range strings.Split(out.String(), "\n") {
					if printers.CellDisplayWidth(line) > 80 {
						t.Fatal("compact output exceeds 80 columns")
					}
				}
			}
			if format == "json" || format == "yaml" {
				data := out.Bytes()
				if format == "yaml" {
					if strings.Contains(out.String(), "\n---") || strings.Contains(out.String(), "\n...") {
						t.Fatal("multiple YAML documents")
					}
					data, err = yaml.YAMLToJSON(data)
					if err != nil {
						t.Fatal(err)
					}
				}
				var value r.DoctorReport
				decoder := json.NewDecoder(bytes.NewReader(data))
				if err := decoder.Decode(&value); err != nil {
					t.Fatal(err)
				}
				var trailing any
				if err := decoder.Decode(&trailing); err != io.EOF {
					t.Fatal("not exactly one machine object")
				}
				if value.Kind != "DoctorReport" || value.Metadata.Namespace != "team-a" || value.Content.Version.SkewState != r.DoctorUnverifiable {
					t.Fatal("wrong typed report")
				}
				structured = append(structured, value)
			}
		}
		if !reflect.DeepEqual(structured[0], structured[1]) {
			t.Fatal("JSON/YAML semantic mismatch")
		}
	}
}

func TestDoctorGoldens(t *testing.T) {
	previous := version.GitVersion
	version.GitVersion = "v1.2.0"
	t.Cleanup(func() { version.GitVersion = previous })
	for _, state := range []string{"complete", "incomplete", "violations"} {
		for _, format := range []string{"table", "wide", "json", "yaml"} {
			t.Run(state+"/"+format, func(t *testing.T) {
				var out bytes.Buffer
				cmd := doctorCommand(newDoctorFactory(), &out, &bytes.Buffer{}, doctorDeps(state))
				cmd.SetArgs([]string{"doctor", "--isvc", "chat", "--ome-namespace", "control", "-o", format})
				err := cmd.Execute()
				wantCode := 0
				if state == "violations" {
					wantCode = 2
				}
				if exitcode.FromError(err) != wantCode {
					t.Fatal(err)
				}
				path := filepath.Join("testdata", "doctor", state+"."+format+".golden")
				want, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("read golden %s: %v\n%s", path, err, &out)
				}
				lines := strings.Split(out.String(), "\n")
				for i := range lines {
					lines[i] = strings.TrimRight(lines[i], " \t")
				}
				if strings.Join(lines, "\n") != string(want) {
					t.Fatalf("golden mismatch %s\n%s", path, &out)
				}
			})
		}
	}
}

func TestDoctorHelpStatesBoundedEvidenceAndExits(t *testing.T) {
	f := newDoctorFactory()
	var out bytes.Buffer
	cmd := doctorCommand(f, &out, &bytes.Buffer{}, doctorDeps("complete"))
	cmd.SetArgs([]string{"doctor", "--help"})
	if err := cmd.Execute(); err != nil || len(f.calls) != 0 {
		t.Fatal("help acquired configuration")
	}
	for _, literal := range []string{"seven fixed", "nine", "10 seconds", "30 seconds", "NotRequested", "Unverifiable", "candidate", "0", "1", "2", "--isvc chat -n team-a --ome-namespace ome"} {
		if !strings.Contains(out.String(), literal) {
			t.Errorf("help lacks %q", literal)
		}
	}
}

type doctorRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn doctorRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return fn(req) }

func TestDoctorProductionCollectorEmitsOnlyFixedExactGETs(t *testing.T) {
	// Exercise real typed REST serializers and request construction without a
	// socket. The transport returns literal fixtures and cannot contact a host.
	for _, selected := range []bool{false, true} {
		for _, format := range []string{"table", "wide", "json", "yaml"} {
			const name = "sk-aaaaaaaaaaaaaaaaaaaa"
			const workload = "sk-bbbbbbbbbbbbbbbbbbbb"
			const ome = "sk-cccccccccccccccccccc"
			paths := []string{}
			var warnings bytes.Buffer
			emitWarning := false
			config := &rest.Config{Host: "https://never-contact.invalid", Timeout: time.Second}
			config.WarningHandler = rest.NewWarningWriter(&warnings, rest.WarningWriterOptions{})
			config.WrapTransport = func(http.RoundTripper) http.RoundTripper {
				return doctorRoundTripFunc(func(req *http.Request) (*http.Response, error) {
					paths = append(paths, req.URL.Path)
					// client-go also advertises the inherited HTTP timeout to the
					// server. No other query (including dryRun) is permitted.
					if req.Method != "GET" || req.URL.RawQuery != "timeout=1s" {
						t.Fatalf("unexpected method/query %s %s", req.Method, req.URL.RawQuery)
					}
					if deadline, ok := req.Context().Deadline(); !ok || time.Until(deadline) > time.Second {
						t.Fatal("request did not preserve smaller configuration timeout")
					}
					status := 200
					var document any
					switch req.URL.Path {
					case "/api/v1":
						document = metav1.APIResourceList{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "pods", Kind: "Pod", Namespaced: true}, {Name: "events", Kind: "Event", Namespaced: true}, {Name: "configmaps", Kind: "ConfigMap", Namespaced: true}}}
					case "/apis/apps/v1":
						document = metav1.APIResourceList{GroupVersion: "apps/v1", APIResources: []metav1.APIResource{{Name: "deployments", Kind: "Deployment", Namespaced: true}}}
					case "/apis/ome.io/v1beta1":
						document = metav1.APIResourceList{GroupVersion: "ome.io/v1beta1", APIResources: []metav1.APIResource{{Name: "inferenceservices", Kind: "InferenceService", Namespaced: true}, {Name: "basemodels", Kind: "BaseModel", Namespaced: true}, {Name: "servingruntimes", Kind: "ServingRuntime", Namespaced: true}, {Name: "clusterbasemodels", Kind: "ClusterBaseModel"}, {Name: "clusterservingruntimes", Kind: "ClusterServingRuntime"}}}
					case "/apis/autoscaling/v2":
						document = metav1.APIResourceList{GroupVersion: "autoscaling/v2"}
					case "/apis/keda.sh/v1alpha1":
						status = 429
						document = metav1.Status{Status: "Failure", Reason: metav1.StatusReasonTooManyRequests, Code: 429, Message: "SECRET https://private.example"}
					case "/apis/gateway.networking.k8s.io/v1":
						document = metav1.APIResourceList{GroupVersion: "gateway.networking.k8s.io/v1"}
					case "/apis/kueue.x-k8s.io/v1beta1":
						document = metav1.APIResourceList{GroupVersion: "kueue.x-k8s.io/v1beta1"}
					case "/apis/apps/v1/namespaces/sk-cccccccccccccccccccc/deployments/ome-controller-manager":
						document = appsv1.Deployment{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}, ObjectMeta: metav1.ObjectMeta{Name: "ome-controller-manager", Namespace: ome}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "sidecar", Image: "SECRET"}, {Name: "manager", Image: "private.example:v1.2.3"}}}}}}
					case "/apis/ome.io/v1beta1/namespaces/sk-bbbbbbbbbbbbbbbbbbbb/inferenceservices/sk-aaaaaaaaaaaaaaaaaaaa":
						if !selected {
							t.Fatal("unselected ISVC GET")
						}
						document = omev1.InferenceService{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"}, ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: workload, UID: "SECRET", Generation: 9}, Status: omev1.InferenceServiceStatus{Traffic: &omev1.TrafficStatus{Algorithm: "SECRET"}}}
					default:
						t.Fatalf("unexpected GET %s", req.URL.Path)
					}
					data, err := json.Marshal(document)
					if err != nil {
						t.Fatal(err)
					}
					headers := http.Header{"Content-Type": {"application/json"}, "Retry-After": {"1"}}
					if emitWarning {
						headers.Set("Warning", `299 private.example "SECRET sk-proj-0123456789abcdefghijklmnopqrstuvwxyz https://private.example"`)
					}
					return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(bytes.NewReader(data)), Request: req}, nil
				})
			}
			f := newDoctorFactory()
			f.NS, f.Context, f.config = workload, "sk-dddddddddddddddddddd", config
			deps := doctorDeps("complete")
			deps.collect = doctorcollection.Collect
			var out bytes.Buffer
			cmd := doctorCommand(f, &out, &bytes.Buffer{}, deps)
			args := []string{"doctor", "--ome-namespace", ome, "-o", format}
			if selected {
				args = append(args, "--isvc", name)
			}
			cmd.SetArgs(args)
			if err := cmd.Execute(); err != nil {
				t.Fatal(err)
			}
			baseline := out.String()
			paths, emitWarning = []string{}, true
			out.Reset()
			cmd = doctorCommand(f, &out, &bytes.Buffer{}, deps)
			cmd.SetArgs(args)
			if err := cmd.Execute(); err != nil {
				t.Fatal(err)
			}
			if warnings.Len() != 0 {
				t.Fatalf("untrusted API warning reached injected stderr writer: %s", &warnings)
			}
			if out.String() != baseline {
				t.Fatalf("%s report changed because of untrusted Warning header", format)
			}
			want := []string{"/api/v1", "/apis/apps/v1", "/apis/ome.io/v1beta1", "/apis/autoscaling/v2", "/apis/keda.sh/v1alpha1", "/apis/gateway.networking.k8s.io/v1", "/apis/kueue.x-k8s.io/v1beta1", "/apis/apps/v1/namespaces/sk-cccccccccccccccccccc/deployments/ome-controller-manager"}
			if selected {
				want = append(want, "/apis/ome.io/v1beta1/namespaces/sk-bbbbbbbbbbbbbbbbbbbb/inferenceservices/sk-aaaaaaaaaaaaaaaaaaaa")
			}
			if !reflect.DeepEqual(paths, want) {
				t.Fatalf("retry/fanout/identity changed: %v", paths)
			}
			for _, bad := range []string{name, workload, ome, f.Context, "SECRET", "private.example"} {
				if strings.Contains(out.String(), bad) {
					t.Fatalf("production path leaked %q", bad)
				}
			}
			if format == "table" || format == "wide" {
				continue
			}
			data := out.Bytes()
			var err error
			if format == "yaml" {
				data, err = yaml.YAMLToJSON(data)
				if err != nil {
					t.Fatal(err)
				}
			}
			var value r.DoctorReport
			if err := json.Unmarshal(data, &value); err != nil {
				t.Fatal(err)
			}
			if len(value.Content.APIs) != 26 || value.Content.HasViolations() {
				t.Fatal("wrong production discovery projection")
			}
			for _, api := range value.Content.APIs {
				if api.Resource == "scaledobjects" && api.Reason != r.DoctorThrottled {
					t.Fatal("optional throttling was not typed diagnostic data")
				}
			}
			for _, feature := range value.Content.Features {
				if feature.Freshness != r.DoctorUnverifiable {
					t.Fatal("invented production feature freshness")
				}
			}
		}
	}
}
