package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	pkgtest "sigs.k8s.io/ome/pkg/utils/testing"
	"sigs.k8s.io/ome/pkg/webhook/admission/isvc"
	"sigs.k8s.io/ome/pkg/webhook/admission/runtimepreset"
	"sigs.k8s.io/ome/pkg/webhook/admission/servingruntime"
)

func testRepository(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestLoadEnvironmentRuntimePresetHooks(t *testing.T) {
	for _, role := range []string{"", "control-plane"} {
		t.Run(role, func(t *testing.T) {
			env, err := loadEnvironment(testRepository(t), "", "127.0.0.1", time.Minute, role)
			if err != nil {
				t.Fatal(err)
			}
			paths := map[string]bool{}
			for _, config := range env.WebhookInstallOptions.MutatingWebhooks {
				for _, hook := range config.Webhooks {
					paths[*hook.ClientConfig.Service.Path] = true
					if hook.FailurePolicy == nil || *hook.FailurePolicy != admissionv1.Fail {
						t.Fatal("runtime preset failure policy was not retained")
					}
				}
			}
			if len(paths) != 2 || !paths["/mutate-ome-io-v1beta1-servingruntime-preset"] || !paths["/mutate-ome-io-v1beta1-clusterservingruntime-preset"] {
				t.Fatalf("unexpected runtime mutators: %v", paths)
			}
		})
	}
}

func TestLoadEnvironmentRejectsMissingPresetConfiguration(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "config", "crd"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(testRepository(t), "config", "crd", "full"), filepath.Join(root, "config", "crd", "full")); err != nil {
		t.Fatal(err)
	}
	objects, err := documents(filepath.Join(testRepository(t), "config", "webhook", "manifests.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var validators []*unstructured.Unstructured
	for _, obj := range objects {
		if obj.GetKind() == "ValidatingWebhookConfiguration" {
			validators = append(validators, obj)
		}
	}
	writeTestObjects(t, filepath.Join(root, "config", "webhook", "manifests.yaml"), validators...)
	_, err = loadEnvironment(root, "", "127.0.0.1", time.Minute, "")
	if err == nil || !strings.Contains(err.Error(), "missing runtime preset webhook") {
		t.Fatalf("missing presets were not reported: %v", err)
	}
}

func writeTestObjects(t *testing.T, path string, objects ...*unstructured.Unstructured) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	var data []byte
	for _, obj := range objects {
		encoded, err := json.Marshal(obj.Object)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, []byte("---\n")...)
		data = append(data, encoded...)
		data = append(data, '\n')
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

// seedShippedPresets creates inferenceservice-config in the namespace the
// preset mutators read, carrying the runtimePresets table the kustomize
// profile ships; the mutators carry no table of their own.
func seedShippedPresets(t *testing.T, api client.Client) {
	t.Helper()
	shipped, err := pkgtest.ShippedInferenceServiceConfigMap()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: constants.OMENamespace}}
	if err := api.Create(ctx, namespace); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace},
		Data:       map[string]string{runtimepreset.PresetsConfigKeyName: shipped.Data[runtimepreset.PresetsConfigKeyName]},
	}
	if err := api.Create(ctx, configMap); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimePresetAdmissionAndDependencyStorage(t *testing.T) {
	env, err := loadEnvironment(testRepository(t), os.Getenv("KUBEBUILDER_ASSETS"), "127.0.0.1", time.Minute, "")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	api, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	seedShippedPresets(t, api)
	decoder := admission.NewDecoder(scheme)
	calls := &trace{}
	server := webhook.NewServer(webhook.Options{Host: env.WebhookInstallOptions.LocalServingHost,
		Port: env.WebhookInstallOptions.LocalServingPort, CertDir: env.WebhookInstallOptions.LocalServingCertDir})
	register := func(path string, handler admission.Handler) {
		server.Register(path, &admission.Webhook{Handler: admission.HandlerFunc(func(ctx context.Context, req admission.Request) admission.Response {
			calls.Lock()
			calls.calls = append(calls.calls, hookCall{Path: path, Operation: string(req.Operation), DryRun: req.DryRun != nil && *req.DryRun})
			calls.Unlock()
			return handler.Handle(ctx, req)
		})})
	}
	register("/mutate-ome-io-v1beta1-servingruntime-preset", &runtimepreset.ServingRuntimeMutator{Decoder: decoder, Clientset: clientset})
	register("/mutate-ome-io-v1beta1-clusterservingruntime-preset", &runtimepreset.ClusterServingRuntimeMutator{Decoder: decoder, Clientset: clientset})
	register("/validate-ome-io-v1beta1-servingruntime", &servingruntime.ServingRuntimeValidator{Client: api, Decoder: decoder})
	register("/validate-ome-io-v1beta1-clusterservingruntime", &servingruntime.ClusterServingRuntimeValidator{Client: api, Decoder: decoder})
	serviceValidator := admission.WithValidator(scheme, &isvc.InferenceServiceValidator{
		Client: api, RuntimeSelector: runtimeselector.New(api)})
	register("/validate-ome-io-v1beta1-inferenceservice", admission.HandlerFunc(func(ctx context.Context, req admission.Request) admission.Response {
		service := &v1beta1.InferenceService{}
		if err := decoder.Decode(req, service); err != nil {
			return admission.Denied(err.Error())
		}
		var dependency v1beta1.ServingRuntime
		if err := api.Get(ctx, client.ObjectKey{Namespace: service.Namespace, Name: service.Spec.Runtime.Name}, &dependency); err != nil {
			return admission.Denied(err.Error())
		}
		if dependency.Spec.DecoderConfig == nil || dependency.Spec.RouterConfig == nil {
			return admission.Denied("runtime dependency has no preset defaults")
		}
		return serviceValidator.Handle(ctx, req)
	}))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- server.Start(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	if err := wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, time.Minute, true, func(context.Context) (bool, error) {
		return server.StartedChecker()(nil) == nil, nil
	}); err != nil {
		t.Fatal(err)
	}

	resource := func(kind, name, engine string) *unstructured.Unstructured {
		obj := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "ome.io/v1beta1", "kind": kind,
			"metadata": map[string]any{"name": name}, "spec": map[string]any{}}}
		if kind != "ClusterServingRuntime" {
			obj.SetNamespace("default")
		}
		if engine != "" {
			obj.SetAnnotations(map[string]string{"ome.io/engine": engine})
		}
		return obj
	}
	updated := resource("ServingRuntime", "updated-runtime", "sglang-pd")
	if err := unstructured.SetNestedField(updated.Object, "explicit-engine", "spec", "engineConfig", "runner", "name"); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(updated.Object, "example.com/engine:v2", "spec", "engineConfig", "runner", "image"); err != nil {
		t.Fatal(err)
	}
	created := resource("ClusterServingRuntime", "new-runtime", "sglang-pd")
	unknown := resource("ClusterServingRuntime", "unknown-runtime", "unknown-engine")
	plain := resource("ServingRuntime", "plain-runtime", "")
	service := resource("InferenceService", "service", "")
	service.Object["spec"] = map[string]any{"runtime": map[string]any{"name": "updated-runtime"},
		"engine": map[string]any{"minReplicas": int64(1), "maxReplicas": int64(1)}}
	root := t.TempDir()
	writeTestObjects(t, filepath.Join(root, "baseline.yaml"), resource("ServingRuntime", "updated-runtime", ""))
	writeTestObjects(t, filepath.Join(root, "catalog.yaml"), resource("ClusterServingRuntime", "catalog-runtime", "sglang-pd"))
	writeTestObjects(t, filepath.Join(root, "proposed.yaml"), updated, created, unknown, plain, service)
	if err := os.WriteFile(filepath.Join(root, "plan.json"), []byte(`{"cluster":"cluster-a"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := seedBaseline(ctx, api, root); err != nil {
		t.Fatal(err)
	}
	results, err := validateAuthored(ctx, api, calls, root, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		isUnknown := strings.Contains(result.File, "unknown-runtime")
		if result.Passed == isUnknown || !result.Unchanged {
			t.Fatalf("unexpected admission result: %+v", result)
		}
		if isUnknown && !strings.Contains(result.Error, "unknown ome.io/engine") {
			t.Fatalf("unexpected engine rejection: %s", result.Error)
		}
		if strings.Contains(result.File, "new-runtime") || strings.Contains(result.File, "updated-runtime") {
			if len(result.HookCalls) != 2 || !strings.HasPrefix(result.HookCalls[0].Path, "/mutate-") || !strings.HasPrefix(result.HookCalls[1].Path, "/validate-") {
				t.Fatalf("mutation did not precede validation: %+v", result.HookCalls)
			}
		}
	}
	for _, obj := range []*unstructured.Unstructured{updated, created, resource("ClusterServingRuntime", "catalog-runtime", "")} {
		stored := &unstructured.Unstructured{}
		stored.SetGroupVersionKind(obj.GroupVersionKind())
		if err := api.Get(ctx, client.ObjectKeyFromObject(obj), stored); err != nil {
			t.Fatal(err)
		}
		for _, component := range []string{"engineConfig", "decoderConfig", "routerConfig"} {
			image, _, _ := unstructured.NestedString(stored.Object, "spec", component, "runner", "image")
			if image == "" {
				t.Fatalf("%s has no %s image after admission", obj.GetName(), component)
			}
			if obj.GetName() == updated.GetName() && component == "engineConfig" && image != "example.com/engine:v2" {
				t.Fatalf("explicit image was overwritten: %s", image)
			}
		}
	}
	if err := api.Get(ctx, client.ObjectKeyFromObject(unknown), unknown); !apierrors.IsNotFound(err) {
		t.Fatalf("unknown engine runtime was stored: %v", err)
	}
	if err := assertIsolation(ctx, api); err != nil {
		t.Fatal(err)
	}
}
