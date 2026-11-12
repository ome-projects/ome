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
	"path"
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
	"RolloutPolicy": true, "AutoscalerPolicy": true,
}

var optionalPolicyKinds = map[string]bool{"RolloutPolicy": true, "AutoscalerPolicy": true}

// splitPlanValidatorPath is the path of the InferenceService Split plan
// validator, which only a control-plane manager serves.
const splitPlanValidatorPath = "/validate-ome-io-v1beta1-inferenceservice-splitplan"

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

// loadEnvironment builds the private API server's environment from the
// rendered inputs under root: the ome.io CRDs, the runtime preset mutators and
// the canonical validators of the supported kinds the role validates.
func loadEnvironment(root, assets, host string, timeout time.Duration, role string) (*envtest.Environment, error) {
	env := newEnvironment(assets, host, timeout)
	crds, err := loadResourceCRDs(root)
	if err != nil {
		return nil, err
	}
	env.CRDs = crds
	hooks, err := loadWebhookConfigurations(filepath.Join(root, "config/webhook/manifests.yaml"), role)
	if err != nil {
		return nil, err
	}
	env.WebhookInstallOptions.MutatingWebhooks = hooks.mutating
	env.WebhookInstallOptions.ValidatingWebhooks = hooks.validating
	return env, nil
}

// newEnvironment is the private API server before its CRDs and webhooks are
// loaded: no existing cluster, the given binaries, one timeout for every
// control plane step, and an API server advertising the loopback host.
func newEnvironment(assets, host string, timeout time.Duration) *envtest.Environment {
	local := false
	env := &envtest.Environment{
		UseExistingCluster: &local, BinaryAssetsDirectory: assets,
		ControlPlaneStartTimeout: timeout, ControlPlaneStopTimeout: timeout,
		CRDInstallOptions:     envtest.CRDInstallOptions{MaxTime: timeout},
		WebhookInstallOptions: envtest.WebhookInstallOptions{LocalServingHost: host, MaxTime: timeout},
	}
	env.ControlPlane.GetAPIServer().Configure().Append("advertise-address", host)
	return env
}

// loadResourceCRDs reads the ome.io CRDs rendered under root. The five common
// kinds and InferenceReplica must all be present: the
// InferenceService validator reads standalone replicas to check name
// collisions.
func loadResourceCRDs(root string) ([]*apiextensions.CustomResourceDefinition, error) {
	paths, err := filepath.Glob(filepath.Join(root, "config/crd/full/*.yaml"))
	if err != nil {
		return nil, err
	}
	var crds []*apiextensions.CustomResourceDefinition
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
			if crd.Spec.Group == "ome.io" {
				crds = append(crds, crd)
			}
		}
	}
	if len(crds) < len(supported)-len(optionalPolicyKinds)+1 {
		return nil, fmt.Errorf("expected the five resource CRDs and InferenceReplica, got %d", len(crds))
	}
	return crds, nil
}

// webhookConfigurations are the configurations the runner installs from the
// rendered manifests: the runtime preset mutators, one validator
// configuration per common kind the role validates, and policy and Split plan
// validators when the selected manifests render them.
type webhookConfigurations struct {
	mutating   []*admissionv1.MutatingWebhookConfiguration
	validating []*admissionv1.ValidatingWebhookConfiguration
}

// loadWebhookConfigurations reads the rendered webhook manifests at path and
// keeps the hooks the runner serves. Each runtime preset mutator must appear
// exactly once, and the common validators must number one per kind the role
// validates. Policy validators follow the selected charts' feature settings.
func loadWebhookConfigurations(path, role string) (webhookConfigurations, error) {
	objects, err := documents(path)
	if err != nil {
		return webhookConfigurations{}, err
	}
	presetPaths := map[string]bool{
		"/mutate-ome-io-v1beta1-servingruntime-preset":        false,
		"/mutate-ome-io-v1beta1-clusterservingruntime-preset": false,
	}
	var hooks webhookConfigurations
	canonical := 0
	for _, obj := range objects {
		switch obj.GetKind() {
		case "MutatingWebhookConfiguration":
			config, err := presetMutators(obj, presetPaths)
			if err != nil {
				return webhookConfigurations{}, err
			}
			if config != nil {
				hooks.mutating = append(hooks.mutating, config)
			}
		case "ValidatingWebhookConfiguration":
			config, err := canonicalValidators(obj, role)
			if err != nil {
				return webhookConfigurations{}, err
			}
			if config != nil {
				hooks.validating = append(hooks.validating, config)
				if servesSupportedKind(config, role) {
					canonical++
				}
			}
		}
	}
	if expected := expectedValidators(role); canonical != expected {
		return webhookConfigurations{}, fmt.Errorf("expected %d canonical webhook configurations, got %d", expected, canonical)
	}
	for path, found := range presetPaths {
		if !found {
			return webhookConfigurations{}, fmt.Errorf("missing runtime preset webhook: %s", path)
		}
	}
	return hooks, nil
}

// presetMutators keeps the runtime preset hooks of one mutating configuration
// and marks each preset path as seen; a preset path seen twice is an error.
// It returns nil when the configuration carries no preset hook.
func presetMutators(obj *unstructured.Unstructured, presetPaths map[string]bool) (*admissionv1.MutatingWebhookConfiguration, error) {
	config := &admissionv1.MutatingWebhookConfiguration{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, config); err != nil {
		return nil, err
	}
	var hooks []admissionv1.MutatingWebhook
	for _, hook := range config.Webhooks {
		path, ok := servicePath(hook.ClientConfig)
		if !ok {
			continue
		}
		seen, preset := presetPaths[path]
		if !preset {
			continue
		}
		if seen {
			return nil, fmt.Errorf("duplicate runtime preset webhook: %s", path)
		}
		presetPaths[path] = true
		hooks = append(hooks, hook)
	}
	if len(hooks) == 0 {
		return nil, nil
	}
	config.Webhooks = hooks
	return config, nil
}

// canonicalValidators keeps the hooks of one validating configuration that
// serve a supported kind under its canonical path, and on a control plane the
// Split plan validator. It returns nil when the configuration carries none.
func canonicalValidators(obj *unstructured.Unstructured, role string) (*admissionv1.ValidatingWebhookConfiguration, error) {
	config := &admissionv1.ValidatingWebhookConfiguration{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, config); err != nil {
		return nil, err
	}
	var hooks []admissionv1.ValidatingWebhook
	for _, hook := range config.Webhooks {
		if path, ok := servicePath(hook.ClientConfig); ok && (validatesSupportedKind(path, role) || role == "control-plane" && path == splitPlanValidatorPath) {
			hooks = append(hooks, hook)
		}
	}
	if len(hooks) == 0 {
		return nil, nil
	}
	config.Webhooks = hooks
	return config, nil
}

// servesSupportedKind reports whether a kept configuration carries the
// canonical validator of a common kind, which the role's count covers.
func servesSupportedKind(config *admissionv1.ValidatingWebhookConfiguration, role string) bool {
	for _, hook := range config.Webhooks {
		if path, ok := servicePath(hook.ClientConfig); ok && validatesSupportedKind(path, role) && !isPolicyValidator(path) {
			return true
		}
	}
	return false
}

// splitPlanValidatorInstalled reports whether the private API server calls
// the Split plan validator, so a control plane's InferenceService probes
// expect a webhook call. Installed hooks address the local server by a URL
// whose path may carry a doubled leading slash.
func splitPlanValidatorInstalled(ctx context.Context, api client.Client) (bool, error) {
	list := &admissionv1.ValidatingWebhookConfigurationList{}
	if err := api.List(ctx, list); err != nil {
		return false, err
	}
	for _, config := range list.Items {
		for _, hook := range config.Webhooks {
			if hook.ClientConfig.URL == nil {
				continue
			}
			if u, err := url.Parse(*hook.ClientConfig.URL); err == nil && path.Clean(u.Path) == splitPlanValidatorPath {
				return true, nil
			}
		}
	}
	return false, nil
}

// validatesSupportedKind reports whether path is the canonical validator path
// of a supported kind the role validates; a control-plane manager validates
// no InferenceService.
func validatesSupportedKind(path, role string) bool {
	for kind := range supported {
		if role == "control-plane" && kind == "InferenceService" {
			continue
		}
		if path == "/validate-ome-io-v1beta1-"+strings.ToLower(kind) {
			return true
		}
	}
	return false
}

// expectedValidators is the number of validator configurations the role
// requires: one per common kind, less the InferenceService on a control plane.
func expectedValidators(role string) int {
	required := len(supported) - len(optionalPolicyKinds)
	if role == "control-plane" {
		return required - 1
	}
	return required
}

func isPolicyValidator(path string) bool {
	for kind := range optionalPolicyKinds {
		if path == "/validate-ome-io-v1beta1-"+strings.ToLower(kind) {
			return true
		}
	}
	return false
}

// servicePath is the service path a hook's client config names, if any.
func servicePath(config admissionv1.WebhookClientConfig) (string, bool) {
	if config.Service == nil || config.Service.Path == nil {
		return "", false
	}
	return *config.Service.Path, true
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

var (
	// releaseRevision is the full commit a release build of the runner embeds.
	releaseRevision = regexp.MustCompile(`^[0-9a-f]{40}$`)
	// shortRevision is a commit as a plan or a manager log names it.
	shortRevision = regexp.MustCompile(`^[0-9a-f]{8,40}$`)
	// imageByDigest is an image reference that names its digest.
	imageByDigest = regexp.MustCompile(`@sha256:[0-9a-f]{64}$`)
)

// runnerFlags supplies the runtime settings and optional coverage requirements.
type runnerFlags struct {
	root, images, assets, output, command, identityName, host string
	metricsAddress, healthAddress                             string
	timeout, identityTTL, headerTimeout, readinessInterval    time.Duration
	requireRevision                                           bool
	requireKinds                                              string
}

func parseRunnerFlags() runnerFlags {
	var f runnerFlags
	flag.StringVar(&f.root, "input-directory", "", "rendered cluster admission inputs")
	flag.StringVar(&f.images, "images", "", "resolved cluster image digests")
	flag.StringVar(&f.assets, "assets", "", "envtest binary directory")
	flag.StringVar(&f.output, "report", "", "JSON report path")
	flag.StringVar(&f.command, "manager-command", "", "prebuilt manager executable")
	flag.StringVar(&f.identityName, "identity-name", "", "private read-only identity name")
	flag.StringVar(&f.host, "loopback-host", "", "IPv4 loopback address")
	flag.DurationVar(&f.timeout, "timeout", 0, "maximum runtime")
	flag.DurationVar(&f.identityTTL, "identity-ttl", 0, "test identity and held-lease lifetime")
	flag.BoolVar(&f.requireRevision, "require-embedded-revision", false, "require a release-stamped runner")
	flag.StringVar(&f.requireKinds, "require-kinds", "", "comma-separated resource kinds the runner must probe")
	flag.StringVar(&f.metricsAddress, "manager-metrics-address", "", "manager metrics address")
	flag.StringVar(&f.healthAddress, "manager-health-address", "", "manager health address")
	flag.DurationVar(&f.headerTimeout, "proxy-header-timeout", 0, "admission trace proxy header timeout")
	flag.DurationVar(&f.readinessInterval, "readiness-poll-interval", 0, "manager readiness poll interval")
	flag.Parse()
	return f
}

// incomplete reports whether a required input is missing or malformed: every
// path, name and address; a positive readiness interval, header timeout and
// timeout; an identity lifetime longer than the timeout; an image that names
// its digest; a hexadecimal revision; and an IPv4 loopback host.
func (f runnerFlags) incomplete(namespace, image, revision string) bool {
	ip := net.ParseIP(f.host)
	return f.root == "" || f.assets == "" || f.output == "" || f.command == "" || namespace == "" || f.identityName == "" ||
		f.metricsAddress == "" || f.healthAddress == "" || f.readinessInterval <= 0 || f.headerTimeout <= 0 ||
		!imageByDigest.MatchString(image) || !shortRevision.MatchString(revision) ||
		ip == nil || !ip.IsLoopback() || ip.To4() == nil || f.timeout <= 0 || f.identityTTL <= f.timeout
}

// clusterImage reads the resolved image digests at path and returns the
// cluster's image, "" when the file names none for it.
func clusterImage(path, cluster string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var images map[string]string
	if err := json.Unmarshal(data, &images); err != nil {
		return "", err
	}
	return images[cluster], nil
}

// apiClient is a client of the private API server with the core kinds.
func apiClient(cfg *rest.Config) (client.Client, error) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	return client.New(cfg, client.Options{Scheme: scheme})
}

// createConfigMaps creates every ConfigMap rendered at path in namespace.
func createConfigMaps(ctx context.Context, api client.Client, path, namespace string) error {
	maps, err := documents(path)
	if err != nil {
		return err
	}
	for _, obj := range maps {
		if obj.GetKind() != "ConfigMap" {
			continue
		}
		obj.SetNamespace(namespace)
		if err := api.Create(ctx, obj); err != nil {
			return err
		}
	}
	return nil
}

// freeLoopbackPort reserves and releases a port on host for the manager's
// webhook server.
func freeLoopbackPort(host string) (int, error) {
	listener, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return 0, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return port, nil
}

// managerArguments is the command line of the prebuilt manager: webhook-only
// on the given port with the private serving certificates, and the
// control-plane role without exec credentials.
func managerArguments(f runnerFlags, role, certDir string, port int) []string {
	args := []string{"--webhook", "--webhook-only", "--webhook-cert-dir=" + certDir, "--webhook-port=" + strconv.Itoa(port), "--metrics-bind-address=" + f.metricsAddress, "--health-probe-addr=" + f.healthAddress, "--zap-encoder=json"}
	if role == "control-plane" {
		args = append(args, "--multicluster-role=control-plane", "--allow-exec-credentials=false")
	}
	return args
}

// managerEnvironment is the manager's environment: the read-only
// kubeconfig, a private temp dir, its namespace, and the process variables
// it needs to find binaries, certificates and libraries.
func managerEnvironment(kubeconfig, dir, namespace string) []string {
	env := []string{"KUBECONFIG=" + kubeconfig, "TMPDIR=" + dir, "POD_NAMESPACE=" + namespace}
	for _, key := range []string{"PATH", "SSL_CERT_FILE", "SSL_CERT_DIR", "LD_LIBRARY_PATH", "LANG", "TZ"} {
		if value, present := os.LookupEnv(key); present {
			env = append(env, key+"="+value)
		}
	}
	return env
}

// startManager starts the prebuilt manager with its output in logs and
// returns the channel that carries its exit and a stop that ends it, waits
// for it and closes the log. On an error nothing is left running or open.
func startManager(ctx context.Context, f runnerFlags, role, certDir string, port int, kubeconfig, dir, namespace string) (<-chan error, func(), error) {
	logs, err := os.Create(filepath.Join(filepath.Dir(f.output), "manager.log"))
	if err != nil {
		return nil, nil, err
	}
	managerCtx, stopManager := context.WithCancel(ctx)
	manager := exec.CommandContext(managerCtx, f.command, managerArguments(f, role, certDir, port)...)
	manager.Env = managerEnvironment(kubeconfig, dir, namespace)
	manager.Stdout, manager.Stderr = logs, logs
	if err := manager.Start(); err != nil {
		stopManager()
		logs.Close()
		return nil, nil, err
	}
	done := make(chan error, 1)
	go func() { done <- manager.Wait(); close(done) }()
	stop := func() {
		stopManager()
		<-done
		logs.Close()
	}
	return done, stop, nil
}

// awaitManagerReady dials the manager's webhook port over TLS at the
// readiness interval until it answers, the manager exits or the run times
// out.
func awaitManagerReady(ctx context.Context, host string, port int, caData []byte, interval time.Duration, exited <-chan error) error {
	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caData)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", net.JoinHostPort(host, strconv.Itoa(port)), &tls.Config{RootCAs: certPool, MinVersion: tls.VersionTLS12})
		if err == nil {
			_ = conn.Close()
			return nil
		}
		select {
		case err := <-exited:
			return fmt.Errorf("prebuilt manager exited: %v; see manager.log", err)
		case <-ctx.Done():
			return fmt.Errorf("manager startup: %w; see manager.log", ctx.Err())
		case <-ticker.C:
		}
	}
}

// embeddedRevision is the commit the manager logged when it started, read
// from the first "Initializing" line of its JSON log; "" when it logged none.
func embeddedRevision(logData []byte) string {
	for _, line := range bytes.Split(logData, []byte{'\n'}) {
		var entry struct {
			Message string `json:"msg"`
			Commit  string `json:"gitCommit"`
		}
		if json.Unmarshal(line, &entry) == nil && entry.Message == "Initializing" {
			return entry.Commit
		}
	}
	return ""
}

// revisionAgreement says what ties the probed manager to the selected
// revision. A runner with an embedded commit must share a prefix with the
// revision, and so must the commit the manager logged, when it logged one.
func revisionAgreement(runnerCommit, embedded, revision string) (string, error) {
	agreement := "development runner without embedded revision"
	if runnerCommit != "" {
		if !(strings.HasPrefix(runnerCommit, revision) || strings.HasPrefix(revision, runnerCommit)) {
			return "", fmt.Errorf("runner revision %s does not match %s", runnerCommit, revision)
		}
		agreement = "matching embedded runner revision and immutable image digest"
	}
	if embedded != "" {
		if !shortRevision.MatchString(embedded) || !(strings.HasPrefix(revision, embedded) || strings.HasPrefix(embedded, revision)) {
			return "", fmt.Errorf("manager reports OME commit %q; expected %s", embedded, revision)
		}
		agreement = "released revision tag, immutable digest, and matching embedded commit"
	}
	return agreement, nil
}

// installWebhooks installs the runtime preset mutators, seeds the baseline
// through them so the validators read a mutated dependency catalog, then
// installs the validators.
func installWebhooks(ctx context.Context, api client.Client, env *envtest.Environment, cfg *rest.Config, hooks webhookConfigurations, root string) error {
	env.WebhookInstallOptions.MutatingWebhooks = hooks.mutating
	if err := env.WebhookInstallOptions.ModifyWebhookDefinitions(); err != nil {
		return err
	}
	if err := env.WebhookInstallOptions.Install(cfg); err != nil {
		return err
	}
	if err := seedBaseline(ctx, api, root); err != nil {
		return err
	}
	env.WebhookInstallOptions.ValidatingWebhooks = hooks.validating
	if err := env.WebhookInstallOptions.ModifyWebhookDefinitions(); err != nil {
		return err
	}
	return env.WebhookInstallOptions.Install(cfg)
}

// writeReport writes the JSON report to path and prints the one-line
// summary.
func writeReport(path, revision string, r report) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		return err
	}
	fmt.Printf("Prebuilt OME %s: startup %.2fs, %d probes %.2fs; controllers disabled\n", revision, r.StartupSecs, len(r.Results), r.ProbeSecs)
	return nil
}

// failedProbe is the first probe that did not pass, as the run's error.
func failedProbe(results []result) error {
	for _, res := range results {
		if !res.Passed {
			return fmt.Errorf("unexpected admission result %s/%s: %s", res.Case, res.File, res.Error)
		}
	}
	return nil
}

// startControlPlane starts the private API server with the CRDs loaded and
// the webhook certificates prepared, and returns the configurations to
// install once the manager serves them.
func startControlPlane(env *envtest.Environment) (*rest.Config, webhookConfigurations, error) {
	hooks := webhookConfigurations{mutating: env.WebhookInstallOptions.MutatingWebhooks, validating: env.WebhookInstallOptions.ValidatingWebhooks}
	if err := env.WebhookInstallOptions.PrepWithoutInstalling(); err != nil {
		return nil, hooks, err
	}
	env.WebhookInstallOptions.ValidatingWebhooks = nil
	env.WebhookInstallOptions.MutatingWebhooks = nil
	fmt.Println("Starting private API server with canonical CRDs and webhook configuration")
	cfg, err := env.Start()
	return cfg, hooks, err
}

// managerRevision reads the commit the manager logged and says how it and
// the runner's own commit agree with the selected revision.
func managerRevision(logPath, revision string) (string, string, error) {
	logData, err := os.ReadFile(logPath)
	if err != nil {
		return "", "", err
	}
	embedded := embeddedRevision(logData)
	agreement, err := revisionAgreement(version.GitCommit, embedded, revision)
	return embedded, agreement, err
}

// probeAndReport runs the authored admission probes, checks that nothing
// but the inputs was stored, writes the report and fails on the first probe
// that did not pass.
func probeAndReport(ctx context.Context, api client.Client, calls *trace, f runnerFlags, role string, r report) error {
	started := time.Now()
	results, err := validateAuthored(ctx, api, calls, f.root, role)
	if err != nil {
		return err
	}
	r.Results = results
	if err := assertIsolation(ctx, api); err != nil {
		return err
	}
	r.ProbeSecs = time.Since(started).Seconds()
	r.ControllersDisabled, r.NoWorkloadObjects = true, true
	if err := writeReport(f.output, r.OMERevision, r); err != nil {
		return err
	}
	return failedProbe(r.Results)
}

func run() error {
	flags := parseRunnerFlags()
	if flags.requireKinds != "" {
		for _, kind := range strings.Split(flags.requireKinds, ",") {
			if !supported[kind] {
				return fmt.Errorf("runner does not probe required kind %q", kind)
			}
		}
	}
	if flags.requireRevision && !releaseRevision.MatchString(version.GitCommit) {
		return fmt.Errorf("runner has no embedded release revision")
	}
	plan, err := readPlan(filepath.Join(flags.root, "plan.json"))
	if err != nil {
		return err
	}
	image, err := clusterImage(flags.images, plan.Cluster)
	if err != nil {
		return err
	}
	if flags.incomplete(plan.ManagerNamespace, image, plan.OMERevision) {
		return fmt.Errorf("required flags include loopback, timeout, selected revision and digest-pinned image")
	}
	env, err := loadEnvironment(flags.root, flags.assets, flags.host, flags.timeout, plan.ManagerRole)
	if err != nil {
		return err
	}
	startup := time.Now()
	cfg, hooks, err := startControlPlane(env)
	defer func() { _ = env.Stop() }()
	if err != nil {
		return err
	}
	api, err := apiClient(cfg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), flags.timeout)
	defer cancel()
	dir, err := os.MkdirTemp("", "prebuilt-admission-manager-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	kubeconfig, err := managerIdentity(ctx, api, cfg, dir, plan.ManagerNamespace, flags.identityName, flags.identityTTL)
	if err != nil {
		return err
	}
	if err := createConfigMaps(ctx, api, filepath.Join(flags.root, "configmap.yaml"), plan.ManagerNamespace); err != nil {
		return err
	}
	port, err := freeLoopbackPort(flags.host)
	if err != nil {
		return err
	}
	exited, stopManager, err := startManager(ctx, flags, plan.ManagerRole, env.WebhookInstallOptions.LocalServingCertDir, port, kubeconfig, dir, plan.ManagerNamespace)
	if err != nil {
		return err
	}
	defer stopManager()
	if err := awaitManagerReady(ctx, flags.host, port, env.WebhookInstallOptions.LocalServingCAData, flags.readinessInterval, exited); err != nil {
		return err
	}
	embedded, agreement, err := managerRevision(filepath.Join(filepath.Dir(flags.output), "manager.log"), plan.OMERevision)
	if err != nil {
		return err
	}
	calls := &trace{}
	proxy, err := startProxy(env.WebhookInstallOptions, port, calls, flags.headerTimeout)
	if err != nil {
		return err
	}
	defer proxy.Close()
	if err := installWebhooks(ctx, api, env, cfg, hooks, flags.root); err != nil {
		return err
	}
	r := report{OMERevision: plan.OMERevision, ManagerImage: image, EmbeddedRevision: embedded, RevisionEvidence: agreement, StartupSecs: time.Since(startup).Seconds()}
	return probeAndReport(ctx, api, calls, flags, plan.ManagerRole, r)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
