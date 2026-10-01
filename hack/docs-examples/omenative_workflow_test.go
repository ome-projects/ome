package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/go-logr/logr"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/render"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/ome/pkg/webhook/admission/isvc"
	"sigs.k8s.io/ome/pkg/webhook/admission/servingruntime"
)

// These checks exercise admission and runtime resolution without an API server.
// They do not establish that an image pulls, a pod starts, or inference works.
func TestRuntimeOnlyDocumentationFixtures(t *testing.T) {
	for _, family := range []string{"omenative-http", "runtime-managed-qwen", "multi-cluster-registration"} {
		t.Run(family, func(t *testing.T) {
			dir := filepath.Join("..", "..", "config", "samples", "docs", family)
			namespace := &corev1.Namespace{}
			rt := &v1beta1.ServingRuntime{}
			service := &v1beta1.InferenceService{}
			readWorkflowYAML(t, filepath.Join(dir, "namespace.yaml"), namespace)
			readWorkflowYAML(t, filepath.Join(dir, "servingruntime.yaml"), rt)
			readWorkflowYAML(t, filepath.Join(dir, "inferenceservice.yaml"), service)

			if namespace.Name == "" || rt.Namespace != namespace.Name || service.Namespace != namespace.Name {
				t.Fatal("runtime and service must use the namespace the example creates")
			}
			if service.Spec.Model != nil || service.Spec.Runtime == nil || service.Spec.Runtime.Kind == nil || *service.Spec.Runtime.Kind != "ServingRuntime" || service.Spec.Runtime.Name != rt.Name {
				t.Fatal("example must name its namespaced runtime without a model resource")
			}

			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := v1beta1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(namespace).Build()
			runtimeValidator := &servingruntime.ServingRuntimeValidator{Client: cl, Decoder: admission.NewDecoder(scheme)}
			raw, err := json.Marshal(rt)
			if err != nil {
				t.Fatal(err)
			}
			response := runtimeValidator.Handle(t.Context(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: admissionv1.Create,
				Name:      rt.Name,
				Namespace: rt.Namespace,
				Object:    runtime.RawExtension{Raw: raw},
			}})
			if !response.Allowed {
				t.Fatalf("ServingRuntime admission rejected the fixture: %v", response.Result)
			}
			if err := cl.Create(t.Context(), rt); err != nil {
				t.Fatal(err)
			}
			selector := runtimeselector.New(cl)
			validator := &isvc.InferenceServiceValidator{Client: cl, Reader: cl, RuntimeSelector: selector}
			if _, err := validator.ValidateCreate(t.Context(), service); err != nil {
				t.Fatalf("InferenceService admission rejected the fixture: %v", err)
			}
			resolve := func(service *v1beta1.InferenceService) *render.Resolved {
				t.Helper()
				resolved, err := render.Resolve(t.Context(), render.Inputs{Service: service, Client: cl, Runtimes: selector, Log: logr.Discard()})
				if err != nil {
					t.Fatalf("resolve the documented runtime: %v", err)
				}
				if resolved.Model != nil || resolved.RuntimeIsCluster || resolved.Specs.Engine == nil || resolved.Specs.Engine.Runner == nil {
					t.Fatal("expected a model-free engine from the namespaced runtime")
				}
				if resolved.Specs.Engine.Runner.Image != rt.Spec.EngineConfig.Runner.Image {
					t.Fatal("engine did not inherit the runtime's image")
				}
				return resolved
			}
			resolve(service)

			if family != "omenative-http" {
				return
			}
			scaled := patchWorkflowService(t, service, `{"spec":{"engine":{"minReplicas":3,"maxReplicas":3}}}`)
			if _, err := validator.ValidateUpdate(t.Context(), service, scaled); err != nil {
				t.Fatalf("documented scale change was rejected: %v", err)
			}
			resolve(scaled)

			updated := patchWorkflowService(t, scaled, `{"spec":{"engine":{"runner":{"name":"ome-container","args":["-listen=:8080","-text=hello from OMENative v2"]}}}}`)
			if _, err := validator.ValidateUpdate(t.Context(), scaled, updated); err != nil {
				t.Fatalf("documented template change was rejected: %v", err)
			}
			resolved := resolve(updated)
			if !slices.Equal(resolved.Specs.Engine.Runner.Args, []string{"-listen=:8080", "-text=hello from OMENative v2"}) {
				t.Fatalf("template change did not replace the arguments: %v", resolved.Specs.Engine.Runner.Args)
			}
		})
	}
}

func readWorkflowYAML(t *testing.T, path string, into any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.UnmarshalStrict(data, into); err != nil {
		t.Fatalf("decode %s against current API types: %v", path, err)
	}
}

func patchWorkflowService(t *testing.T, original *v1beta1.InferenceService, patch string) *v1beta1.InferenceService {
	t.Helper()
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	patched, err := jsonpatch.MergePatch(raw, []byte(patch))
	if err != nil {
		t.Fatal(err)
	}
	service := &v1beta1.InferenceService{}
	if err := json.Unmarshal(patched, service); err != nil {
		t.Fatal(err)
	}
	return service
}
