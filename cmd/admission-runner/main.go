package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	admissionapi "k8s.io/api/admission/v1"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"sigs.k8s.io/ome/pkg/version"
)

type hookCall struct {
	Path      string `json:"path"`
	Operation string `json:"operation"`
	DryRun    bool   `json:"dryRun"`
}

type trace struct {
	sync.Mutex
	calls []hookCall
}

func (t *trace) snapshot() []hookCall {
	t.Lock()
	defer t.Unlock()
	return append([]hookCall(nil), t.calls...)
}

type result struct {
	Case      string     `json:"case"`
	File      string     `json:"file"`
	Allowed   bool       `json:"allowed"`
	Passed    bool       `json:"passed"`
	Error     string     `json:"error,omitempty"`
	HookCalls []hookCall `json:"hookCalls"`
	Unchanged bool       `json:"storedObjectUnchanged"`
}

type report struct {
	OMERevision         string   `json:"expectedOMERevision"`
	StartupSecs         float64  `json:"startupSeconds"`
	ProbeSecs           float64  `json:"probeSeconds"`
	Results             []result `json:"results"`
	ManagerImage        string   `json:"managerImage"`
	ControllersDisabled bool     `json:"controllersDisabled"`
	NoWorkloadObjects   bool     `json:"noWorkloadObjects"`
	EmbeddedRevision    string   `json:"embeddedRevision,omitempty"`
	RevisionEvidence    string   `json:"revisionEvidence"`
}

var supported = map[string]bool{
	"InferenceService": true, "ServingRuntime": true, "ClusterServingRuntime": true,
	"BaseModel": true, "ClusterBaseModel": true,
}

func documents(path string) ([]*unstructured.Unstructured, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	decoder := utilyaml.NewYAMLOrJSONDecoder(f, 4096)
	var objects []*unstructured.Unstructured
	for {
		obj := &unstructured.Unstructured{}
		if err := decoder.Decode(obj); err != nil {
			if err == io.EOF {
				return objects, nil
			}
			return nil, err
		}
		if obj.GetKind() != "" {
			objects = append(objects, obj)
		}
	}
}

func loadEnvironment(root, assets, host string, timeout time.Duration, role string) (*envtest.Environment, error) {
	local := false
	env := &envtest.Environment{
		UseExistingCluster: &local, BinaryAssetsDirectory: assets,
		ControlPlaneStartTimeout: timeout, ControlPlaneStopTimeout: timeout,
		CRDInstallOptions:     envtest.CRDInstallOptions{MaxTime: timeout},
		WebhookInstallOptions: envtest.WebhookInstallOptions{LocalServingHost: host, MaxTime: timeout},
	}
	env.ControlPlane.GetAPIServer().Configure().Append("advertise-address", host)
	paths, err := filepath.Glob(filepath.Join(root, "config/crd/full/*.yaml"))
	if err != nil {
		return nil, err
	}
	for _, path := range paths {
		objects, err := documents(path)
		if err != nil {
			return nil, err
		}
		for _, obj := range objects {
			crd := &apiextensions.CustomResourceDefinition{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, crd); err != nil {
				return nil, err
			}
			// The ISVC validator also reads standalone replicas to check name collisions.
			if crd.Spec.Group == "ome.io" {
				env.CRDs = append(env.CRDs, crd)
			}
		}
	}
	if len(env.CRDs) < len(supported)+1 {
		return nil, fmt.Errorf("expected the five resource CRDs and InferenceReplica, got %d", len(env.CRDs))
	}
	objects, err := documents(filepath.Join(root, "config/webhook/manifests.yaml"))
	if err != nil {
		return nil, err
	}
	presetPaths := map[string]bool{
		"/mutate-ome-io-v1beta1-servingruntime-preset":        false,
		"/mutate-ome-io-v1beta1-clusterservingruntime-preset": false,
	}
	for _, obj := range objects {
		if obj.GetKind() == "MutatingWebhookConfiguration" {
			config := &admissionv1.MutatingWebhookConfiguration{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, config); err != nil {
				return nil, err
			}
			var hooks []admissionv1.MutatingWebhook
			for _, hook := range config.Webhooks {
				if hook.ClientConfig.Service == nil || hook.ClientConfig.Service.Path == nil {
					continue
				}
				path := *hook.ClientConfig.Service.Path
				if seen, supported := presetPaths[path]; supported {
					if seen {
						return nil, fmt.Errorf("duplicate runtime preset webhook: %s", path)
					}
					presetPaths[path] = true
					hooks = append(hooks, hook)
				}
			}
			if len(hooks) != 0 {
				config.Webhooks = hooks
				env.WebhookInstallOptions.MutatingWebhooks = append(env.WebhookInstallOptions.MutatingWebhooks, config)
			}
			continue
		}
		if obj.GetKind() != "ValidatingWebhookConfiguration" {
			continue
		}
		config := &admissionv1.ValidatingWebhookConfiguration{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, config); err != nil {
			return nil, err
		}
		var hooks []admissionv1.ValidatingWebhook
		for _, hook := range config.Webhooks {
			if hook.ClientConfig.Service == nil || hook.ClientConfig.Service.Path == nil {
				continue
			}
			path := *hook.ClientConfig.Service.Path
			for kind := range supported {
				if role == "control-plane" && kind == "InferenceService" {
					continue
				}
				if path == "/validate-ome-io-v1beta1-"+strings.ToLower(kind) {
					hooks = append(hooks, hook)
				}
			}
		}
		if len(hooks) != 0 {
			config.Webhooks = hooks
			env.WebhookInstallOptions.ValidatingWebhooks = append(env.WebhookInstallOptions.ValidatingWebhooks, config)
		}
	}
	expected := len(supported)
	if role == "control-plane" {
		expected--
	}
	if len(env.WebhookInstallOptions.ValidatingWebhooks) != expected {
		return nil, fmt.Errorf("expected %d canonical webhook configurations, got %d", expected, len(env.WebhookInstallOptions.ValidatingWebhooks))
	}
	for path, found := range presetPaths {
		if !found {
			return nil, fmt.Errorf("missing runtime preset webhook: %s", path)
		}
	}
	return env, nil
}

func probeResource(ctx context.Context, api client.Client, calls *trace, original *unstructured.Unstructured, webhookExpected bool) (result, error) {
	obj := original.DeepCopy()
	before := &unstructured.Unstructured{}
	before.SetGroupVersionKind(obj.GroupVersionKind())
	key := client.ObjectKeyFromObject(obj)
	getErr := api.Get(ctx, key, before)
	if getErr != nil && !apierrors.IsNotFound(getErr) {
		return result{}, getErr
	}
	start := len(calls.snapshot())
	var err error
	if apierrors.IsNotFound(getErr) {
		err = api.Create(ctx, obj, client.DryRunAll, client.FieldValidation(metav1.FieldValidationStrict))
	} else {
		obj.SetResourceVersion(before.GetResourceVersion())
		err = api.Update(ctx, obj, client.DryRunAll, client.FieldValidation(metav1.FieldValidationStrict))
	}
	r := result{File: resourceKey(original), Allowed: err == nil, HookCalls: calls.snapshot()[start:]}
	if err != nil {
		r.Error = err.Error()
	}
	after := &unstructured.Unstructured{}
	after.SetGroupVersionKind(obj.GroupVersionKind())
	afterErr := api.Get(ctx, key, after)
	r.Unchanged = apierrors.IsNotFound(getErr) && apierrors.IsNotFound(afterErr) || getErr == nil && afterErr == nil && reflect.DeepEqual(before.Object, after.Object)
	r.Passed = r.Allowed && r.Unchanged && webhookExpected == (len(r.HookCalls) > 0)
	for _, call := range r.HookCalls {
		if !call.DryRun {
			r.Passed = false
		}
	}
	return r, nil
}

func managerIdentity(ctx context.Context, api client.Client, cfg *rest.Config, dir, namespace, identityName string, duration time.Duration) (string, error) {
	for _, object := range []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: identityName, Namespace: namespace}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: identityName}, Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"*"}, Resources: []string{"*"}, Verbs: []string{"get", "list", "watch"}},
			{NonResourceURLs: []string{"*"}, Verbs: []string{"get"}},
		}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: identityName},
			RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: identityName},
			Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: identityName, Namespace: namespace}}},
	} {
		if err := api.Create(ctx, object); err != nil {
			return "", err
		}
	}

	typed, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", err
	}
	ttl := int64(duration.Seconds())
	token, err := typed.CoreV1().ServiceAccounts(namespace).CreateToken(ctx, identityName, &authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: &ttl}}, metav1.CreateOptions{})
	if err != nil {
		return "", err
	}
	kcfg := clientcmdapi.Config{
		Clusters:  map[string]*clientcmdapi.Cluster{"local": {Server: cfg.Host, CertificateAuthorityData: cfg.CAData}},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{"readonly": {Token: token.Status.Token}},
		Contexts:  map[string]*clientcmdapi.Context{"local": {Cluster: "local", AuthInfo: "readonly", Namespace: namespace}}, CurrentContext: "local",
	}
	data, err := clientcmd.Write(kcfg)
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "manager.kubeconfig")
	if err := os.WriteFile(path, data, 0600); err != nil {
		return "", err
	}
	readerConfig, err := clientcmd.RESTConfigFromKubeConfig(data)
	if err != nil {
		return "", err
	}
	reader, err := client.New(readerConfig, client.Options{})
	if err != nil {
		return "", err
	}
	denied := reader.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: identityName + "-write-probe", Namespace: namespace}})
	if !apierrors.IsForbidden(denied) {
		return "", fmt.Errorf("manager identity unexpectedly permits writes: %v", denied)
	}
	return path, nil
}

func startProxy(options envtest.WebhookInstallOptions, targetPort int, calls *trace, headerTimeout time.Duration) (*http.Server, error) {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(options.LocalServingCAData) {
		return nil, fmt.Errorf("missing webhook CA")
	}
	target, err := url.Parse("https://" + net.JoinHostPort(options.LocalServingHost, strconv.Itoa(targetPort)))
	if err != nil {
		return nil, err
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var review admissionapi.AdmissionReview
		if err := json.Unmarshal(data, &review); err != nil || review.Request == nil {
			http.Error(w, "invalid admission request", http.StatusBadRequest)
			return
		}
		calls.Lock()
		calls.calls = append(calls.calls, hookCall{r.URL.Path, string(review.Request.Operation), review.Request.DryRun != nil && *review.Request.DryRun})
		calls.Unlock()
		r.Body = io.NopCloser(bytes.NewReader(data))
		proxy.ServeHTTP(w, r)
	})
	server := &http.Server{Addr: net.JoinHostPort(options.LocalServingHost, strconv.Itoa(options.LocalServingPort)), Handler: handler, ReadHeaderTimeout: headerTimeout}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return nil, err
	}
	go func() {
		_ = server.ServeTLS(listener, filepath.Join(options.LocalServingCertDir, "tls.crt"), filepath.Join(options.LocalServingCertDir, "tls.key"))
	}()
	return server, nil
}

func assertIsolation(ctx context.Context, api client.Client) error {
	for _, kind := range []string{"PodList", "ServiceList", "InferenceReplicaList"} {
		list := &unstructured.UnstructuredList{}
		version := "v1"
		if kind == "InferenceReplicaList" {
			version = "ome.io/v1beta1"
		}
		list.SetAPIVersion(version)
		list.SetKind(kind)
		if err := api.List(ctx, list); err != nil {
			return err
		}
		// The API server creates its own default kubernetes Service.
		for _, obj := range list.Items {
			if kind == "ServiceList" && obj.GetNamespace() == "default" && obj.GetName() == "kubernetes" {
				continue
			}
			return fmt.Errorf("unexpected workload object: %s %s/%s", kind, obj.GetNamespace(), obj.GetName())
		}
	}
	return nil
}

func run() error {
	root := flag.String("input-directory", "", "rendered cluster admission inputs")
	imagesPath := flag.String("images", "", "resolved cluster image digests")
	assets := flag.String("assets", "", "envtest binary directory")
	output := flag.String("report", "", "JSON report path")
	command := flag.String("manager-command", "", "prebuilt manager executable")
	identityName := flag.String("identity-name", "", "private read-only identity name")
	host := flag.String("loopback-host", "", "IPv4 loopback address")
	timeout := flag.Duration("timeout", 0, "maximum runtime")
	identityTTL := flag.Duration("identity-ttl", 0, "test identity and held-lease lifetime")
	requireRevision := flag.Bool("require-embedded-revision", false, "require a release-stamped runner")
	metricsAddress := flag.String("manager-metrics-address", "", "manager metrics address")
	healthAddress := flag.String("manager-health-address", "", "manager health address")
	headerTimeout := flag.Duration("proxy-header-timeout", 0, "admission trace proxy header timeout")
	readinessInterval := flag.Duration("readiness-poll-interval", 0, "manager readiness poll interval")
	flag.Parse()
	if *requireRevision && !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(version.GitCommit) {
		return fmt.Errorf("runner has no embedded release revision")
	}
	planPath := filepath.Join(*root, "plan.json")
	plan, err := readPlan(planPath)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(*imagesPath)
	if err != nil {
		return err
	}
	var images map[string]string
	if err := json.Unmarshal(data, &images); err != nil {
		return err
	}
	imageValue := images[plan.Cluster]
	image, namespace, revision := &imageValue, &plan.ManagerNamespace, &plan.OMERevision
	config := filepath.Join(*root, "configmap.yaml")
	role := plan.ManagerRole
	ip := net.ParseIP(*host)
	if *root == "" || *assets == "" || *output == "" || *command == "" || *namespace == "" || *identityName == "" || *metricsAddress == "" || *healthAddress == "" || *readinessInterval <= 0 || *headerTimeout <= 0 || !regexp.MustCompile(`@sha256:[0-9a-f]{64}$`).MatchString(*image) || !regexp.MustCompile(`^[0-9a-f]{8,40}$`).MatchString(*revision) || ip == nil || !ip.IsLoopback() || ip.To4() == nil || *timeout <= 0 || *identityTTL <= *timeout {
		return fmt.Errorf("required flags include loopback, timeout, selected revision and digest-pinned image")
	}

	env, err := loadEnvironment(*root, *assets, *host, *timeout, role)
	if err != nil {
		return err
	}
	startup := time.Now()
	savedValidators := env.WebhookInstallOptions.ValidatingWebhooks
	savedMutators := env.WebhookInstallOptions.MutatingWebhooks
	if err := env.WebhookInstallOptions.PrepWithoutInstalling(); err != nil {
		return err
	}
	env.WebhookInstallOptions.ValidatingWebhooks = nil
	env.WebhookInstallOptions.MutatingWebhooks = nil
	fmt.Println("Starting private API server with canonical CRDs and webhook configuration")
	cfg, err := env.Start()
	defer func() { _ = env.Stop() }()
	if err != nil {
		return err
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	api, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	dir, err := os.MkdirTemp("", "prebuilt-admission-manager-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	kubeconfig, err := managerIdentity(ctx, api, cfg, dir, *namespace, *identityName, *identityTTL)
	if err != nil {
		return err
	}
	maps, err := documents(config)
	if err != nil {
		return err
	}
	for _, obj := range maps {
		if obj.GetKind() != "ConfigMap" {
			continue
		}
		obj.SetNamespace(*namespace)

		if err := api.Create(ctx, obj); err != nil {
			return err
		}
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(*host, "0"))
	if err != nil {
		return err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	logs, err := os.Create(filepath.Join(filepath.Dir(*output), "manager.log"))
	if err != nil {
		return err
	}
	defer logs.Close()
	managerCtx, stopManager := context.WithCancel(ctx)
	defer stopManager()
	manager := exec.CommandContext(managerCtx, *command, "--webhook", "--webhook-only", "--webhook-cert-dir="+env.WebhookInstallOptions.LocalServingCertDir, "--webhook-port="+strconv.Itoa(port), "--metrics-bind-address="+*metricsAddress, "--health-probe-addr="+*healthAddress, "--zap-encoder=json")
	if role == "control-plane" {
		manager.Args = append(manager.Args, "--multicluster-role=control-plane", "--allow-exec-credentials=false")
	}
	manager.Env = []string{"KUBECONFIG=" + kubeconfig, "TMPDIR=" + dir, "POD_NAMESPACE=" + *namespace}
	for _, key := range []string{"PATH", "SSL_CERT_FILE", "SSL_CERT_DIR", "LD_LIBRARY_PATH", "LANG", "TZ"} {
		if value, present := os.LookupEnv(key); present {
			manager.Env = append(manager.Env, key+"="+value)
		}
	}
	manager.Stdout, manager.Stderr = logs, logs
	if err := manager.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- manager.Wait(); close(done) }()
	defer func() { stopManager(); <-done }()
	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(env.WebhookInstallOptions.LocalServingCAData)
	ticker := time.NewTicker(*readinessInterval)
	defer ticker.Stop()
	for {
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", net.JoinHostPort(*host, strconv.Itoa(port)), &tls.Config{RootCAs: certPool, MinVersion: tls.VersionTLS12})
		if err == nil {
			_ = conn.Close()
			break
		}
		select {
		case err := <-done:
			return fmt.Errorf("prebuilt manager exited: %v; see manager.log", err)
		case <-ctx.Done():
			return fmt.Errorf("manager startup: %w; see manager.log", ctx.Err())
		case <-ticker.C:
		}
	}
	logData, err := os.ReadFile(logs.Name())
	if err != nil {
		return err
	}
	embedded := ""
	for _, line := range bytes.Split(logData, []byte{'\n'}) {
		var entry struct {
			Message string `json:"msg"`
			Commit  string `json:"gitCommit"`
		}
		if json.Unmarshal(line, &entry) == nil && entry.Message == "Initializing" {
			embedded = entry.Commit
			break
		}
	}
	evidence := "development runner without embedded revision"
	if version.GitCommit != "" {
		if !(strings.HasPrefix(version.GitCommit, *revision) || strings.HasPrefix(*revision, version.GitCommit)) {
			return fmt.Errorf("runner revision %s does not match %s", version.GitCommit, *revision)
		}
		evidence = "matching embedded runner revision and immutable image digest"
	}
	if embedded != "" {
		if !regexp.MustCompile(`^[0-9a-f]{8,40}$`).MatchString(embedded) || !(strings.HasPrefix(*revision, embedded) || strings.HasPrefix(embedded, *revision)) {
			return fmt.Errorf("manager reports OME commit %q; expected %s", embedded, *revision)
		}
		evidence = "released revision tag, immutable digest, and matching embedded commit"
	}
	calls := &trace{}
	proxy, err := startProxy(env.WebhookInstallOptions, port, calls, *headerTimeout)
	if err != nil {
		return err
	}
	defer proxy.Close()
	// Seed runtime presets through mutation before validators read the dependency catalog.
	env.WebhookInstallOptions.MutatingWebhooks = savedMutators
	if err := env.WebhookInstallOptions.ModifyWebhookDefinitions(); err != nil {
		return err
	}
	if err := env.WebhookInstallOptions.Install(cfg); err != nil {
		return err
	}
	if err := seedBaseline(ctx, api, *root); err != nil {
		return err
	}
	env.WebhookInstallOptions.ValidatingWebhooks = savedValidators
	if err := env.WebhookInstallOptions.ModifyWebhookDefinitions(); err != nil {
		return err
	}
	if err := env.WebhookInstallOptions.Install(cfg); err != nil {
		return err
	}
	r := report{OMERevision: *revision, ManagerImage: *image, EmbeddedRevision: embedded, RevisionEvidence: evidence, StartupSecs: time.Since(startup).Seconds()}
	started := time.Now()
	results, err := validateAuthored(ctx, api, calls, *root, role)
	if err != nil {
		return err
	}
	r.Results = results

	if err := assertIsolation(ctx, api); err != nil {
		return err
	}
	r.ProbeSecs = time.Since(started).Seconds()
	r.ControllersDisabled, r.NoWorkloadObjects = true, true
	data, err = json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(*output, append(data, '\n'), 0600); err != nil {
		return err
	}
	fmt.Printf("Prebuilt OME %s: startup %.2fs, %d probes %.2fs; controllers disabled\n", *revision, r.StartupSecs, len(r.Results), r.ProbeSecs)
	for _, res := range r.Results {
		if !res.Passed {
			return fmt.Errorf("unexpected admission result %s/%s: %s", res.Case, res.File, res.Error)
		}
	}
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
