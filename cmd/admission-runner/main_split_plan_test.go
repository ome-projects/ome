package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/webhook/admission/splitplan"
)

var splitMembers = []string{"member-a", "member-b", "member-c"}

func TestLoadEnvironmentKeepsSplitPlanValidatorOnControlPlane(t *testing.T) {
	six, withPlan := lockGroups(6), lockWith(lockCanonicalHooks(), lockValidating("splitplan", splitPlanValidatorPath))
	cp, err := lockRoot(t, "control-plane", six, withPlan...)
	if err != nil || len(cp.WebhookInstallOptions.ValidatingWebhooks) != 5 {
		t.Fatalf("a control plane keeps its four validators and the Split plan validator: %v", err)
	}
	member, err := lockRoot(t, "", six, withPlan...)
	if err != nil || len(member.WebhookInstallOptions.ValidatingWebhooks) != 5 {
		t.Fatalf("a member keeps its five validators and drops the Split plan validator: %v", err)
	}
	for _, config := range member.WebhookInstallOptions.ValidatingWebhooks {
		if config.Name == "splitplan" {
			t.Fatal("a member kept the Split plan validator")
		}
	}
	if _, err := lockRoot(t, "control-plane", six, lockWith(lockCanonicalHooks()[:5], lockValidating("splitplan", splitPlanValidatorPath))...); err == nil || err.Error() != "expected 4 canonical webhook configurations, got 3" {
		t.Fatalf("the Split plan validator does not stand in for a canonical one: %v", err)
	}
}

// TestControlPlaneRefusesHeldSplitPlan probes a control plane whose proposed
// resources register three clusters: a Split service every cluster can carry
// is admitted, and one whose two decoders cannot cover the three clusters
// holding engines is refused with the planner's reason.
func TestControlPlaneRefusesHeldSplitPlan(t *testing.T) {
	root := splitPlanRoot(t)
	env, err := loadEnvironment(root, os.Getenv("KUBEBUILDER_ASSETS"), "127.0.0.1", time.Minute, "control-plane")
	if err != nil {
		t.Fatal(err)
	}
	api, calls := startSplitPlanServer(t, env)
	writeTestObjects(t, filepath.Join(root, "proposed.yaml"), append(splitRegistrations(t), splitService(t, "carried", 5), splitService(t, "held", 2))...)
	writeTestObjects(t, filepath.Join(root, "baseline.yaml"))
	writeTestObjects(t, filepath.Join(root, "catalog.yaml"))
	if err := os.WriteFile(filepath.Join(root, "plan.json"), []byte(`{"cluster":"hub","managerRole":"control-plane"}`), 0600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := seedBaseline(ctx, api, root); err != nil {
		t.Fatal(err)
	}
	results, err := validateAuthored(ctx, api, calls, root, "control-plane")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("want the two services probed, got %+v", results)
	}
	for _, result := range results {
		held := strings.HasSuffix(result.File, "/held")
		if result.Passed == held || len(result.HookCalls) != 1 || result.HookCalls[0].Path != splitPlanValidatorPath {
			t.Fatalf("unexpected admission result: %+v", result)
		}
		if held && !strings.Contains(result.Error, "decoder minimum 2 cannot give each of the 3 ") {
			t.Fatalf("the refusal does not carry the planner's reason: %s", result.Error)
		}
	}
}

// splitPlanRoot is an input root with the repository's CRDs and webhook
// manifests plus the Split plan validator configuration the chart renders on
// a control plane.
func splitPlanRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "config", "crd"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(testRepository(t), "config", "crd", "full"), filepath.Join(root, "config", "crd", "full")); err != nil {
		t.Fatal(err)
	}
	hooks, err := documents(filepath.Join(testRepository(t), "config", "webhook", "manifests.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	writeTestObjects(t, filepath.Join(root, "config", "webhook", "manifests.yaml"), append(hooks, splitPlanConfiguration(t))...)
	return root
}

// splitPlanConfiguration is the Split plan validator configuration as the
// chart renders it on a control plane.
func splitPlanConfiguration(t *testing.T) *unstructured.Unstructured {
	t.Helper()
	path, ignore, none := splitPlanValidatorPath, admissionv1.Ignore, admissionv1.SideEffectClassNone
	return toUnstructured(t, &admissionv1.ValidatingWebhookConfiguration{
		TypeMeta:   metav1.TypeMeta{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration"},
		ObjectMeta: metav1.ObjectMeta{Name: "inferenceservice-splitplan.ome.io"},
		Webhooks: []admissionv1.ValidatingWebhook{{
			Name:                    "inferenceservice.ome-webhook-server.splitplan-validator",
			ClientConfig:            admissionv1.WebhookClientConfig{Service: &admissionv1.ServiceReference{Name: "ome-webhook-server-service", Namespace: "ome", Path: &path}},
			FailurePolicy:           &ignore,
			SideEffects:             &none,
			AdmissionReviewVersions: []string{"v1"},
			Rules: []admissionv1.RuleWithOperations{{
				Operations: []admissionv1.OperationType{admissionv1.Create, admissionv1.Update},
				Rule:       admissionv1.Rule{APIGroups: []string{"ome.io"}, APIVersions: []string{"v1beta1"}, Resources: []string{"inferenceservices"}},
			}},
		}},
	})
}

// startSplitPlanServer starts the private API server and a webhook server
// that serves the Split plan validator and records each call.
func startSplitPlanServer(t *testing.T, env *envtest.Environment) (client.Client, *trace) {
	return startTestWebhookServer(t, env, func(api client.Client, scheme *runtime.Scheme) map[string]admission.Handler {
		return map[string]admission.Handler{
			splitPlanValidatorPath: admission.WithValidator[*v1beta1.InferenceService](scheme, &splitplan.Validator{Reader: api}),
		}
	})
}

func startTestWebhookServer(t *testing.T, env *envtest.Environment, handlers func(client.Client, *runtime.Scheme) map[string]admission.Handler) (client.Client, *trace) {
	t.Helper()
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
	seedShippedPresets(t, api)
	calls := &trace{}
	server := webhook.NewServer(webhook.Options{Host: env.WebhookInstallOptions.LocalServingHost,
		Port: env.WebhookInstallOptions.LocalServingPort, CertDir: env.WebhookInstallOptions.LocalServingCertDir})
	for path, handler := range handlers(api, scheme) {
		server.Register(path, &admission.Webhook{Handler: admission.HandlerFunc(func(ctx context.Context, req admission.Request) admission.Response {
			calls.Lock()
			calls.calls = append(calls.calls, hookCall{Path: path, Operation: string(req.Operation), DryRun: req.DryRun != nil && *req.DryRun})
			calls.Unlock()
			return handler.Handle(ctx, req)
		})})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	done := make(chan error, 1)
	go func() { done <- server.Start(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	if err := wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, time.Minute, true, func(context.Context) (bool, error) {
		return server.StartedChecker()(nil) == nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	return api, calls
}

func splitRegistrations(t *testing.T) []*unstructured.Unstructured {
	t.Helper()
	var out []*unstructured.Unstructured
	for _, name := range splitMembers {
		out = append(out, toUnstructured(t, &v1beta1.WorkloadCluster{
			TypeMeta:   metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "WorkloadCluster"},
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: v1beta1.WorkloadClusterSpec{ClusterSource: v1beta1.ClusterConnectionSource{
				KubeConfig: &v1beta1.KubeConfigSource{SecretRef: corev1.SecretReference{Name: name, Namespace: "ome"}},
			}},
		}))
	}
	return out
}

// splitService is a Split service with 6 engines, the given decoder minimum
// and 3 routers, divided equally over the three registered clusters.
func splitService(t *testing.T, name string, decoders int) *unstructured.Unstructured {
	t.Helper()
	even := &v1beta1.ReplicaRatio{Min: ptr.To[int32](1), Max: ptr.To[int32](1)}
	var terms []v1beta1.ClusterAffinityTerm
	for _, member := range splitMembers {
		terms = append(terms, v1beta1.ClusterAffinityTerm{
			Weights:     &v1beta1.ClusterAffinityWeights{Default: even, Decoder: even, Router: even},
			MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{member}}},
		})
	}
	runner := func() *v1beta1.RunnerSpec {
		return &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Image: "img"}}
	}
	return toUnstructured(t, &v1beta1.InferenceService{
		TypeMeta:   metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: v1beta1.InferenceServiceSpec{
			Placement: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSplit, ClusterAffinity: terms},
			Engine:    &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(6)}, Runner: runner()},
			Decoder:   &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(decoders)}, Runner: runner()},
			Router:    &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(3)}, Runner: runner()},
		},
	})
}

func toUnstructured(t *testing.T, obj runtime.Object) *unstructured.Unstructured {
	t.Helper()
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatal(fmt.Errorf("convert %T: %w", obj, err))
	}
	u := &unstructured.Unstructured{Object: content}
	unstructured.RemoveNestedField(u.Object, "metadata", "creationTimestamp")
	unstructured.RemoveNestedField(u.Object, "status")
	return u
}
