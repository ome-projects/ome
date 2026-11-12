package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"text/template"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/webhook/admission/autoscalerpolicy"
	"sigs.k8s.io/ome/pkg/webhook/admission/rolloutpolicy"
)

func TestPolicyAdmissionAndDependencyStorage(t *testing.T) {
	env, err := loadEnvironment(policyTestRoot(t), os.Getenv("KUBEBUILDER_ASSETS"), "127.0.0.1", time.Minute, "")
	if err != nil {
		t.Fatal(err)
	}
	// Only policy writes are probed; keep their configurations from the loader.
	env.WebhookInstallOptions.MutatingWebhooks = nil
	hooks := env.WebhookInstallOptions.ValidatingWebhooks[:0]
	for _, config := range env.WebhookInstallOptions.ValidatingWebhooks {
		if strings.Contains(config.Name, "rolloutpolicy") || strings.Contains(config.Name, "autoscalerpolicy") {
			hooks = append(hooks, config)
		}
	}
	if len(hooks) != 2 {
		t.Fatalf("policy webhook configurations missing: %v", hooks)
	}
	env.WebhookInstallOptions.ValidatingWebhooks = hooks
	api, calls := startTestWebhookServer(t, env, func(api client.Client, scheme *runtime.Scheme) map[string]admission.Handler {
		decoder := admission.NewDecoder(scheme)
		return map[string]admission.Handler{
			"/validate-ome-io-v1beta1-rolloutpolicy":    &rolloutpolicy.Validator{Client: api, Decoder: decoder},
			"/validate-ome-io-v1beta1-autoscalerpolicy": &autoscalerpolicy.Validator{Client: api, Decoder: decoder},
		}
	})
	invalidRollout := rolloutTestPolicy("invalid-rollout")
	invalidRollout.Spec.Canary.Steps[1].Traffic = 90
	invalidAutoscaler := autoscalerTestPolicy("invalid-autoscaler")
	invalidAutoscaler.Spec.Keda.Triggers[0].Metadata["query"] = "("
	updatedRollout := rolloutTestPolicy("updated-rollout")
	updatedRollout.Spec.Canary.Steps[0].Traffic = 5
	updatedRollout.Spec.Canary.Steps[0].Capacity = intstr.FromString("5%")
	updatedAutoscaler := autoscalerTestPolicy("updated-autoscaler")
	updatedAutoscaler.Spec.Keda.Triggers[0].Metadata["query"] = "{{ .MaxReplicas }} + 1"
	kindChange := rolloutTestPolicy("referenced-rollout")
	kindChange.Spec = v1beta1.RolloutPolicySpec{BlueGreen: &v1beta1.GroupBlueGreen{}}
	consumer := toUnstructured(t, &v1beta1.InferenceService{
		TypeMeta:   metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"},
		ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "default"},
		Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}, Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, PolicyRef: &v1beta1.RolloutPolicyRef{Name: kindChange.Name, Progression: v1beta1.RolloutProgressionCanary},
		}}}},
	})
	root := t.TempDir()
	writeTestObjects(t, filepath.Join(root, "baseline.yaml"), toUnstructured(t, rolloutTestPolicy("updated-rollout")), toUnstructured(t, autoscalerTestPolicy("updated-autoscaler")), toUnstructured(t, rolloutTestPolicy("referenced-rollout")))
	writeTestObjects(t, filepath.Join(root, "catalog.yaml"), consumer)
	writeTestObjects(t, filepath.Join(root, "proposed.yaml"), toUnstructured(t, rolloutTestPolicy("new-rollout")), toUnstructured(t, autoscalerTestPolicy("new-autoscaler")), toUnstructured(t, updatedRollout), toUnstructured(t, updatedAutoscaler), toUnstructured(t, invalidRollout), toUnstructured(t, invalidAutoscaler), toUnstructured(t, kindChange))
	if err := os.WriteFile(filepath.Join(root, "plan.json"), []byte(`{"cluster":"cluster-a"}`), 0600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := seedBaseline(ctx, api, root); err != nil {
		t.Fatal(err)
	}
	results, err := validateAuthored(ctx, api, calls, root, "")
	if err != nil {
		t.Fatal(err)
	}
	checkPolicyResults(t, api, results)
	unknown := toUnstructured(t, rolloutTestPolicy("unknown-field"))
	if err := unstructured.SetNestedField(unknown.Object, true, "spec", "unknownField"); err != nil {
		t.Fatal(err)
	}
	res, err := probeResource(ctx, api, calls, unknown, true)
	if err != nil || res.Allowed || !res.Unchanged || !strings.Contains(res.Error, "unknown field") {
		t.Fatalf("unknown policy field was not rejected by the API: %+v, %v", res, err)
	}
}

func checkPolicyResults(t *testing.T, api client.Client, results []result) {
	t.Helper()
	ctx := context.Background()
	if len(results) != 7 {
		t.Fatalf("expected seven policy probes, got %+v", results)
	}
	for _, res := range results {
		denied := strings.Contains(res.File, "invalid-") || strings.Contains(res.File, "referenced-rollout")
		if res.Passed == denied || !res.Unchanged || len(res.HookCalls) != 1 || !res.HookCalls[0].DryRun {
			t.Fatalf("unexpected policy result: %+v", res)
		}
		if strings.Contains(res.File, "updated-") || strings.Contains(res.File, "referenced-rollout") {
			if res.HookCalls[0].Operation != "UPDATE" {
				t.Fatalf("baseline policy was not updated: %+v", res)
			}
		}
		if strings.Contains(res.File, "referenced-rollout") && !strings.Contains(res.Error, "progression kind cannot change") {
			t.Fatalf("unexpected referenced-policy denial: %+v", res)
		}
		if !denied {
			obj := &unstructured.Unstructured{}
			parts := strings.Split(res.File, "/")
			obj.SetAPIVersion("ome.io/v1beta1")
			obj.SetKind(parts[0])
			if err := api.Get(ctx, client.ObjectKey{Namespace: parts[1], Name: parts[2]}, obj); err != nil {
				t.Fatalf("accepted policy unavailable to later probes: %v", err)
			}
		}
	}
}

func TestLoadEnvironmentPolicyHooksFollowSelectedCharts(t *testing.T) {
	root := policyTestRoot(t)
	for _, role := range []string{"", "control-plane"} {
		env, err := loadEnvironment(root, "", "127.0.0.1", time.Minute, role)
		if err != nil {
			t.Fatal(err)
		}
		if len(env.WebhookInstallOptions.ValidatingWebhooks) != expectedValidators(role)+2 {
			t.Fatalf("%q: selected policy hooks missing", role)
		}
	}
}

func policyTestRoot(t *testing.T) string {
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
	for _, kind := range []string{"rolloutpolicy", "autoscalerpolicy"} {
		data, err := os.ReadFile(filepath.Join(testRepository(t), "charts", "ome-resources", "templates", "ome-controller", "webhooks", kind+"validator.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		tmpl, err := template.New(kind).Parse(string(data))
		if err != nil {
			t.Fatal(err)
		}
		var rendered bytes.Buffer
		values := map[string]any{"Values": map[string]any{"ome": map[string]any{
			"rolloutPolicy": map[string]any{"enabled": true}, "autoscalerPolicy": map[string]any{"enabled": true},
		}}, "Release": map[string]any{"Namespace": "ome"}}
		if err := tmpl.Execute(&rendered, values); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, kind+".yaml")
		if err := os.WriteFile(path, rendered.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		objects, err := documents(path)
		if err != nil {
			t.Fatal(err)
		}
		hooks = append(hooks, objects...)
	}
	writeTestObjects(t, filepath.Join(root, "config", "webhook", "manifests.yaml"), hooks...)
	return root
}

func TestRunRejectsUnsupportedRequiredKind(t *testing.T) {
	if err := lockRun(t, map[string]string{"require-kinds": "RolloutPolicy,Pod"}); err == nil || !strings.Contains(err.Error(), `does not probe required kind "Pod"`) {
		t.Fatalf("unsupported coverage requirement accepted: %v", err)
	}
}

func rolloutTestPolicy(name string) *v1beta1.RolloutPolicy {
	return &v1beta1.RolloutPolicy{
		TypeMeta:   metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "RolloutPolicy"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: v1beta1.RolloutPolicySpec{Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("10%"), Traffic: 10},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}}},
	}
}
func autoscalerTestPolicy(name string) *v1beta1.AutoscalerPolicy {
	return &v1beta1.AutoscalerPolicy{
		TypeMeta:   metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "AutoscalerPolicy"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: v1beta1.AutoscalerPolicySpec{Enforcement: v1beta1.PolicyEnforcementDefault, Class: v1beta1.AutoscalerKEDA,
			Keda: &v1beta1.KedaPolicyTemplate{Triggers: []v1beta1.KedaTriggerTemplate{{Type: "prometheus",
				ProviderRef: &v1beta1.MetricProviderRef{Name: "cluster-prometheus"}, MetricType: autoscalingv2.AverageValueMetricType,
				QueryReturnsDesiredReplicas: true, Metadata: map[string]string{
					"threshold": "1", "ignoreNullValues": "false", "query": "{{ .MaxReplicas }}",
				},
			}}}},
	}
}
